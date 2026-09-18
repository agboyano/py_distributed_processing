from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterable
from typing import Any

import fs_structs

from .connector import Connector


def sleep(a: float, b: float | None = None) -> None:
    "Sleeps `a` seconds, or a uniform random time in [a, b] if `b` is given."
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


logger = logging.getLogger(__name__)


class FileSystemConnector(Connector):
    """Transport on a shared directory (NFS, local disk, ...) via `fs_structs`.

    Queues are `fs_structs` lists and the registry (sets and counters) is a
    `fs_structs` dict, all under an `FSNamespace` rooted at `base_path`.
    Registry operations run under a file lock (`registry_lock`); the id
    counters under their own (`nclients_lock`, `nservers_lock`). Blocking
    pops wait for filesystem events (watchdog) or poll, depending on
    `with_watchdog`.

    Args:
        base_path (str): Directory shared by clients and workers.
        temp_dir (str, optional): Temporary directory used by the
            namespace for atomic writes. Defaults to None.
        serializer: `fs_structs` serializer (`dump(obj, path)` / `load(path)`)
            used to store queue items and registry entries on disk. It is
            the only encoding step: `enqueue` receives, and the `pop*`
            methods return, Python objects. Defaults to
            `fs_structs.structs.joblib_serializer`.

    Attributes:
        with_watchdog (bool): If True (default), blocking pops wait for
            file-creation events; if False, they poll.
        pop_sleep (tuple): (min, max) seconds of the uniform random polling
            delay. Only used when `with_watchdog` is False.
        pop_watchdog_timeout (float): Seconds to wait for a file event
            before re-checking the queue.
        pop_sleep_watchdog (tuple): (min, max) seconds of the uniform
            random wait after a file event, to minimize the probability of
            race conditions.
        pop_race_wait (tuple): (min, max) seconds of the uniform random
            wait, in `pop_multiple`, after another worker took the element
            this worker was about to pop, before trying the next one.
            `FSList.pop_left` takes no lock (fs_structs >= 0.0.5).
        registry_timeout (float): Seconds to wait for registry reads.
        lock_registry_timeout (float): Seconds to wait for the registry lock.
        lock_registry_watchdog_timeout (float): Seconds to wait for a file
            event while waiting for the registry lock.
        lock_registry_wait (tuple): (min, max) seconds of the uniform
            random wait between registry lock attempts.
        lock_registry_max_age (float): A registry lock older than this many
            seconds is treated as left by a dead process and broken. Use
            minutes, not seconds, and keep the machine clocks in sync.

    """

    sep = "_"
    id_prefix = "fs"

    def __init__(
        self,
        base_path: str,
        temp_dir: str | None = None,
        serializer=fs_structs.structs.joblib_serializer,
    ):
        self.namespace = fs_structs.structs.FSNamespace(base_path, temp_dir, serializer)
        self.registry = self.namespace.udict("registry")
        self.with_watchdog = True

        self.pop_sleep = (5, 10)  # only used when with_watchdog is False

        self.pop_timeout: float = 60
        self.pop_watchdog_timeout: float = 60
        self.pop_sleep_watchdog = (0.0, 0.1)

        self.pop_race_wait = (0.0, 0.1)

        self.registry_timeout: float = 10

        self.lock_registry_timeout: float = 60
        self.lock_registry_watchdog_timeout: float = 10
        self.lock_registry_wait = (0.0, 0.1)
        self.lock_registry_max_age: float = 600

    def clean_namespace(self) -> None:
        "Deletes every object linked to the namespace (queues, registry, counters)."
        self.namespace.clear()
        self.registry = self.namespace.udict("registry")

    # ---- primitives ----------------------------------------------------------

    def _lock(self, name: str):
        return fs_structs.structs.lock_context(
            self.registry.base_path,
            name,
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
            max_age=self.lock_registry_max_age,
        )

    def _registry_lock(self):
        return self._lock("registry_lock")

    def _incr(self, key: str) -> int:
        with self._lock(f"{key}_lock"):
            n = self.registry.get(key, 0) + 1
            self.registry[key] = n
        return n

    def _set_add(self, key: str, members: Iterable[str]) -> None:
        self.registry[key] = self.registry.get(key, set()).union(members)

    def _set_discard(self, key: str, members: Iterable[str]) -> int:
        current = self.registry.get(key, set())
        remaining = current.difference(members)
        removed = len(current) - len(remaining)
        if removed:
            self.registry[key] = remaining
        return removed

    def _set_members(self, key: str) -> set[str]:
        return set(self.registry.get(key, set()))

    def _set_keys(self, prefix: str) -> list[str]:
        return [k for k in self.registry.keys() if k.startswith(prefix)]

    def _set_delete(self, key: str) -> None:
        try:
            del self.registry[key]
        except KeyError:
            pass

    # ---- queues --------------------------------------------------------------

    def enqueue(self, queue_name: str, msg: Any) -> None:
        "Appends a message to the queue (stored with the `fs_structs` serializer)."
        queue = self.namespace.list(queue_name)
        queue.append(msg)

    def _pop_loop(
        self, try_pop: Callable[[], Any], watch_paths: list, timeout: float
    ) -> tuple | None:
        """Calls `try_pop` until it returns a message or `timeout` expires.

        Between attempts it waits for a file-creation event in `watch_paths`
        (watchdog mode) or sleeps (polling mode). `timeout < 0` waits
        indefinitely, `0` tries once.
        """
        if ok := try_pop():
            return ok

        start_time = time.time()
        time_left = timeout
        wait_forever = timeout < -0.001
        while time_left > 0.0 or wait_forever:
            watchdog_timeout = (
                self.pop_watchdog_timeout
                if wait_forever
                else min(self.pop_watchdog_timeout, time_left)
            )
            if self.with_watchdog:
                _ = fs_structs.watchdog.wait_until_file_event(
                    watch_paths, [], ["created"], timeout=watchdog_timeout
                )
                # Wait a random time to minimize the probability of races.
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)  # Standard polling delay

            if ok := try_pop():
                return ok

            time_left = timeout - (time.time() - start_time)
        return None  # Timeout reached

    def pop(self, queue_name: str, timeout: float = -1) -> tuple | None:
        """Blocking pop of the first item of a FIFO queue. Used by clients.

        Args:
            queue_name: Queue reference.
            timeout: < 0 waits indefinitely, 0 tries once, > 0 waits at
                most that many seconds.

        Returns:
            tuple: (queue_name, value), or None on timeout.

        """
        queue = self.namespace.list(queue_name)

        def try_pop():
            try:
                # pop(0) instead of pop_left: only one client per responses queue.
                return (queue_name, queue.pop(0))
            except (IndexError, KeyError):
                return False

        return self._pop_loop(try_pop, [queue.base_path], timeout)

    def pop_multiple(self, queue_names: list, timeout: float = -1) -> tuple | None:
        """Blocking pop from multiple FIFO queues in priority order. Used by workers.

        Args:
            queue_names: Queue references, highest priority first.
            timeout: < 0 waits indefinitely, 0 tries once, > 0 waits at
                most that many seconds.

        Returns:
            tuple: (queue_name, value), or None on timeout.

        """
        queue_refs = [(q, self.namespace.list(q)) for q in queue_names]

        def try_pop():
            for q_name, queue in queue_refs:
                try:
                    return (q_name, queue.pop_left(wait=self.pop_race_wait))
                except (IndexError, KeyError):
                    continue
            return False

        return self._pop_loop(try_pop, [q.base_path for _, q in queue_refs], timeout)

    def pop_all(self, queue_name: str) -> list:
        "Pops every available message of the queue, in order. Used by clients."
        queue = self.namespace.list(queue_name)
        N = len(queue)
        # pop(0) instead of pop_left: only one client per responses queue.
        return [queue.pop(0) for _ in range(N)]
