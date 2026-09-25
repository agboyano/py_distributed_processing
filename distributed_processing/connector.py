"""Base class and behavioural contract for transports (connectors).

A connector gives `Client` and `Worker` four things inside a *namespace*:

- FIFO queues of Python objects (requests and responses),
- a small set store for the registry (which queues serve each method and
  which workers listen on each queue),
- two counters, for unique client and worker ids,
- a key/value store of shared variables (`set_variable`/`get_variable`),
- worker heartbeats (`heartbeat`/`alive_workers`), stored in the same
  key/value store under their own key family.

`Connector` implements naming and the registry once, on top of a handful of
primitives that each transport provides. The class docstring states the
rules every implementation must follow; `tests/test_connector.py` checks
them against every connector.
"""

from __future__ import annotations

import logging
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

logger = logging.getLogger(__name__)

# Key families of the registry (set store):
#   {METHOD_QUEUES}{sep}{method}    -> {queue_ref, ...}  queues serving a method
#   {WORKERS_QUEUE}{sep}{queue_ref} -> {worker_id, ...}  workers listening on a queue
# Key families of the value store:
#   {VARIABLES}{sep}{name}          -> value                shared variables
#   {HEARTBEATS}{sep}{worker_id}    -> time.time() of the last heartbeat
#   {HEARTBEAT_INTERVALS}{sep}{worker_id} -> seconds between heartbeats
METHOD_QUEUES = "method_queues"
WORKERS_QUEUE = "workers_queue"
VARIABLES = "variables"
HEARTBEATS = "heartbeats"
HEARTBEAT_INTERVALS = "heartbeat_intervals"

# A worker is dead after this many missed heartbeats, when its interval
# is known: one missed beat and a few seconds of clock skew between
# machines do not count.
HEARTBEAT_TOLERANCE = 3

# Seconds without a heartbeat after which a worker is considered dead,
# base value of `Connector.default_heartbeat_max_age`. It is the floor: a
# worker that publishes a longer interval gets `HEARTBEAT_TOLERANCE` times
# its interval instead. Three times the default heartbeat interval (10 s).
DEFAULT_HEARTBEAT_MAX_AGE = 30.0

# Sentinel for "no default given" in `update_variable`, so that an explicit
# `default=None` is still a valid default.
MISSING = object()


class Connector(ABC):
    """Transport contract: FIFO queues, a set store, counters and variables in a namespace.

    Subclasses implement the primitives (the abstract methods) and inherit
    the naming scheme and the registry. `Client` and `Worker` only use the
    public methods.

    Rules every connector follows:

    Names
        Keys and queue references are built by `_key(*parts)`, which joins
        with `sep` and, if the transport needs it, prefixes a namespace.
        `requests_queue_name(get_requests_queue(q)) == q` for any simple
        name `q` without `sep`. Request ids are `{client_id}:{n}` (built by
        `Client`); a client id may itself contain ``:`` (Redis ids do), so
        `get_reply_to_from_id` splits on the last one and
        `get_reply_to_from_id(f"{cid}:{n}") == get_responses_queue(cid)`.
        Ids from `get_client_id` and `get_server_id` are unique for the life
        of the namespace: a counter that only `clean_namespace` resets.

    Queues
        `enqueue` appends; each queue is FIFO. `pop` and `pop_multiple`
        return `(queue_ref, obj)` or None on timeout: `timeout < 0` waits
        indefinitely, `timeout == 0` checks once without waiting and
        `timeout > 0` waits at most that many seconds. `pop_multiple`
        checks the queues in the given order (highest priority first) and
        returns the first message found. `pop_all` never blocks and returns
        `[obj, ...]` in FIFO order. A message the serializer cannot decode
        is logged at ERROR and skipped: it is never raised and does not
        extend the wait. A responses queue has a single consumer; a requests
        queue may have many, and each message reaches exactly one of them.

    Registry
        `register_methods` is additive and idempotent (sets).
        `unregister_methods(worker_id)` removes the worker from every queue;
        a queue left without workers is dropped and removed from every
        method, and a method left without queues is dropped. This is
        coarse-grained: a method stays available while any queue serving it
        still has a worker, even if that worker never offered the method.
        `methods_registry` and `workers_registry` return plain dicts of
        lists (snapshots). `random_queue_for_method` returns None when no
        queue serves the method.

    Variables
        `set_variable(name, value)` stores a Python object under `name`,
        shared by every client and worker of the namespace.
        `get_variable(name, default=None)` returns a *copy* of it (the
        value travels through the serializer, so mutating the returned
        object changes nothing shared), or `default` if the name is not
        set. `delete_variable(name)` returns whether the name existed and
        `variables()` lists the names. Each call is atomic on its own and
        the last write wins: no lock and no expiry.
        `update_variable(name, fn, default=MISSING)` is the one atomic
        read-modify-write: it runs `fn(current)` and stores the result
        while holding `_variable_lock(key)`, a lock per variable that
        transports shared by several processes must provide (the default
        is a no-op). If the variable is not set, `fn` receives `default`,
        or `KeyError` is raised when no default was given. A variable
        updated with `update_variable` must not be written with
        `set_variable`, which bypasses the lock. Variables live
        in their own key family, apart from the registry, and are not
        covered by `_registry_lock()`.

    Heartbeats
        `heartbeat(worker_id)` stores the writer's `time.time()` under the
        worker id, in its own key family of the value store (so it never
        shows up in `variables()`). The worker sends its heartbeat
        interval with each beat; `heartbeats()` returns `{worker_id: time}`
        and `heartbeat_intervals()` returns `{worker_id: seconds}`.
        `dead_workers(max_age)` holds the rule: a worker is dead when its
        last heartbeat is older than `max_age`, or older than
        `HEARTBEAT_TOLERANCE` times its own interval if that is longer,
        measured with the reader's clock. `alive_workers(max_age)` returns
        the ids that are not dead. `delete_heartbeat(worker_id)` removes
        the keys; `Worker.close()` does it, a worker that dies or stops
        without `close()` leaves a heartbeat that goes stale.
        `prune_dead_workers(max_age)` unregisters and deletes the dead
        workers. A registered worker **without** a heartbeat key is never
        pruned and is not reported as dead: it may be an older version or a
        worker with heartbeats disabled. No transport expiry (Redis
        `EXPIRE`) is used, so the rule is the same on every transport.
        Clocks of the machines are assumed to agree within a few seconds;
        `max_age` is the floor that absorbs that skew and covers workers
        that do not publish their interval. A `max_age` left unset is
        `default_heartbeat_max_age`, and a `Worker` without
        `heartbeat_interval` beats every `default_heartbeat_interval`
        seconds: both are class attributes a transport may override.

    Atomicity
        Every public registry operation runs inside `_registry_lock()`.
        Transports whose set primitives are atomic by themselves (Redis)
        keep the default no-op lock; the others (a shared filesystem) return
        a real lock. `_incr` must be atomic on its own. Queue operations are
        not covered by the lock: the transport itself must deliver each
        message to exactly one consumer.

    Namespaces
        `clean_namespace` deletes queues, registry, counters, variables and
        heartbeats, so ids start again from 1.

    Encoding
        Queues and variables carry Python objects. The connector owns the
        encoding (its serializer); every client and worker of a namespace
        must use the same one.

    Attributes:
        sep (str): Separator used to build keys and queue references.
        id_prefix (str): Prefix of client and worker ids
            (`{id_prefix}_client`, `{id_prefix}_server`).
        default_heartbeat_interval (float): Seconds between heartbeats of a
            `Worker` created without `heartbeat_interval`. 10 s.
        default_heartbeat_max_age (float): `max_age` used when a caller
            leaves it unset (`dead_workers`, `alive_workers`,
            `prune_dead_workers`, their `Client` versions and `gather`).
            30 s.

    """

    sep: str = "_"
    id_prefix: str = "connector"

    # Heartbeat defaults live on the connector because they depend on the
    # transport, not on the code: a shared drive is slower than Redis and
    # every beat is a file write. A worker and a client on the same
    # transport then agree without any configuration. A transport
    # overrides both when its latency asks for it (see
    # `FileSystemConnector`).
    default_heartbeat_interval: float = 10.0
    default_heartbeat_max_age: float = DEFAULT_HEARTBEAT_MAX_AGE

    # ---- primitives every transport implements ------------------------------

    @abstractmethod
    def clean_namespace(self) -> None:
        "Deletes every queue, registry entry, counter, variable and heartbeat of the namespace."

    @abstractmethod
    def _incr(self, key: str) -> int:
        """Atomically increments the counter `key` and returns its new value.

        The first call on a fresh namespace returns 1.
        """

    @abstractmethod
    def _set_add(self, key: str, members: Iterable[str]) -> None:
        "Adds `members` to the set `key`, creating the set if needed."

    @abstractmethod
    def _set_discard(self, key: str, members: Iterable[str]) -> int:
        "Removes `members` from the set `key`. Returns how many were present."

    @abstractmethod
    def _set_members(self, key: str) -> set[str]:
        "Returns the members of the set `key` (empty if it does not exist)."

    @abstractmethod
    def _set_keys(self, prefix: str) -> list[str]:
        "Returns the keys of every set whose name starts with `prefix`."

    @abstractmethod
    def _set_delete(self, key: str) -> None:
        "Deletes the set `key`. No error if it does not exist."

    @abstractmethod
    def _value_set(self, key: str, value: Any) -> None:
        "Stores the Python object `value` under `key`, replacing any previous one."

    @abstractmethod
    def _value_get(self, key: str) -> Any:
        "Returns the object stored under `key`. Raises KeyError if there is none."

    @abstractmethod
    def _value_delete(self, key: str) -> bool:
        "Deletes `key`. Returns True if it existed, False otherwise."

    @abstractmethod
    def _value_keys(self, prefix: str) -> list[str]:
        "Returns the keys of every stored value whose name starts with `prefix`."

    @abstractmethod
    def enqueue(self, queue: str, msg: Any) -> None:
        "Appends `msg` (a Python object) to the FIFO queue `queue`."

    @abstractmethod
    def pop(self, queue: str, timeout: float = -1) -> tuple | None:
        """Pops the first message of `queue`. Used by clients (single consumer).

        Args:
            queue (str): Queue reference.
            timeout (float): < 0 waits indefinitely, 0 checks once, > 0
                waits at most that many seconds. Defaults to -1.

        Returns:
            tuple: `(queue, obj)`, or None on timeout.

        """

    @abstractmethod
    def pop_multiple(self, queues: list, timeout: float = -1) -> tuple | None:
        """Pops the first message found in `queues`, checked in order. Used by workers.

        Args:
            queues (list): Queue references, highest priority first.
            timeout (float): < 0 waits indefinitely, 0 checks once, > 0
                waits at most that many seconds. Defaults to -1.

        Returns:
            tuple: `(queue, obj)`, or None on timeout.

        """

    @abstractmethod
    def pop_all(self, queue: str) -> list:
        "Returns every message available in `queue`, in FIFO order, without waiting."

    # ---- hooks with a default ------------------------------------------------

    def _key(self, *parts: str) -> str:
        "Builds a key or queue reference from its parts."
        return self.sep.join(parts)

    def _registry_lock(self) -> AbstractContextManager:
        "Context manager held during registry operations. No-op by default."
        return nullcontext()

    def _variable_lock(self, key: str) -> AbstractContextManager:
        """Context manager held by `update_variable` around the read-modify-write of `key`.

        No-op by default. A transport shared by several processes must
        return a real lock, one per variable (a lock directory on the
        filesystem, a Redis lock).
        """
        return nullcontext()

    # ---- names ---------------------------------------------------------------

    def get_requests_queue(self, queue_name: str) -> str:
        "Returns the requests queue reference for a simple queue name."
        return self._key("requests", queue_name)

    def requests_queue_name(self, queue_ref: str) -> str:
        "Returns the simple queue name for a requests queue reference."
        return queue_ref.removeprefix(self._key("requests") + self.sep)

    def get_responses_queue(self, client_id: str) -> str:
        "Returns the responses queue reference for a client id."
        return f"{client_id}{self.sep}responses"

    def get_reply_to_from_id(self, id_str: str) -> str:
        "Derives the responses queue from a request id `{client_id}:{n}`."
        return self.get_responses_queue(id_str.rsplit(":", 1)[0])

    def get_client_id(self) -> str:
        "Generates a new unique client id, `{id_prefix}_client{sep}{n}`."
        return self._new_id("client")

    def get_server_id(self) -> str:
        "Generates a new unique worker id, `{id_prefix}_server{sep}{n}`."
        return self._new_id("server")

    def _new_id(self, kind: str) -> str:
        n = self._incr(self._key(f"n{kind}s"))
        return f"{self._key(f'{self.id_prefix}_{kind}')}{self.sep}{n}"

    # ---- registry ------------------------------------------------------------

    def register_methods(self, requests_queues_dict: dict, worker_id: str) -> None:
        """Publishes the worker's methods and the queues where they are served.

        Args:
            requests_queues_dict (dict): `{queue_ref: {method_name: fn, ...}, ...}`.
            worker_id (str): Id of the worker publishing the methods.

        """
        methods: dict = {}
        for queue_ref, func_dict in requests_queues_dict.items():
            for method in func_dict:
                methods.setdefault(method, []).append(queue_ref)

        with self._registry_lock():
            for method, queues in methods.items():
                self._set_add(self._key(METHOD_QUEUES, method), queues)
                logger.info(
                    f"Method {method} published as available for queues: {', '.join(queues)}"
                )
            for queue_ref in requests_queues_dict:
                self._set_add(self._key(WORKERS_QUEUE, queue_ref), [worker_id])

    def unregister_methods(self, worker_id: str) -> None:
        """Removes a worker from the registry.

        Queues left without workers are dropped and removed from every
        method; methods left without queues are dropped.
        """
        with self._registry_lock():
            empty_queues = self._discard_and_prune(
                WORKERS_QUEUE, [worker_id], "Queue", "workers"
            )
            if empty_queues:
                self._discard_and_prune(METHOD_QUEUES, empty_queues, "Method", "queues")

    def _discard_and_prune(
        self, family: str, members: list, kind: str, listeners: str
    ) -> list:
        "Discards `members` from every set of `family`; deletes and returns the emptied ones."
        prefix = self._key(family) + self.sep
        emptied = []
        for key in self._set_keys(prefix):
            if self._set_discard(key, members) and not self._set_members(key):
                self._set_delete(key)
                name = key.removeprefix(prefix)
                emptied.append(name)
                logger.info(
                    f"{kind} {name} has no remaining public {listeners} listening. Unregistered."
                )
        return emptied

    def methods_registry(self) -> dict:
        "Returns `{method: [queue_ref, ...]}`. Used by clients."
        return self._family_snapshot(METHOD_QUEUES)

    def workers_registry(self) -> dict:
        "Returns `{queue_ref: [worker_id, ...]}`. Used by clients."
        return self._family_snapshot(WORKERS_QUEUE)

    def _family_snapshot(self, family: str) -> dict:
        prefix = self._key(family) + self.sep
        with self._registry_lock():
            return {
                key.removeprefix(prefix): list(self._set_members(key))
                for key in self._set_keys(prefix)
            }

    def all_queues_for_method(self, method: str) -> list:
        "Returns every queue ref where requests for `method` can be sent."
        return list(self._set_members(self._key(METHOD_QUEUES, method)))

    def random_queue_for_method(self, method: str) -> str | None:
        "Returns a random queue ref serving `method`, or None if there is none."
        available = self.all_queues_for_method(method)
        return random.choice(available) if available else None

    # ---- variables -----------------------------------------------------------

    def set_variable(self, name: str, value: Any) -> None:
        """Stores `value` under `name`, shared by the whole namespace.

        The value goes through the connector's serializer, so it must be
        encodable by it (JSON on Redis by default). The last write wins.

        Note:
            No lock is taken, on purpose: a set is a single atomic write
            (one rename, one Redis SET) and the usual case is one writer
            publishing a parameter for many readers. So a set that lands
            while an `update_variable` of the same name is in flight can be
            overwritten by that update. A variable that is updated with
            `update_variable` should only be written with `update_variable`.

        Args:
            name (str): Variable name.
            value: Python object to share.

        """
        self._value_set(self._key(VARIABLES, name), value)

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Returns a copy of the shared variable `name`, or `default` if not set.

        Mutating the returned object does not change the shared value: call
        `set_variable` again to publish a change.

        Args:
            name (str): Variable name.
            default: Value returned when the variable is not set.
                Defaults to None.

        """
        try:
            return self._value_get(self._key(VARIABLES, name))
        except KeyError:
            return default

    def update_variable(self, name: str, fn: Callable, default: Any = MISSING) -> Any:
        """Atomically replaces the shared variable `name` with `fn(current)`.

        The read, the call and the write happen while holding the lock of
        that variable, so concurrent updates from several processes are
        serialized and none is lost. Keep `fn` pure and quick: it runs with
        the lock held. Do not mix with `set_variable` on the same name, it
        does not take the lock.

        Args:
            name (str): Variable name.
            fn (callable): Receives the current value and returns the new one.
            default: Value passed to `fn` when the variable is not set. If
                not given, a missing variable raises `KeyError` instead
                (`None` is a valid default when passed explicitly).

        Returns:
            The new value.

        Raises:
            KeyError: If the variable is not set and no `default` was given.
            Whatever `fn` raises; the variable is then left unchanged.
                A transport may also raise its lock error on timeout.

        """
        key = self._key(VARIABLES, name)
        with self._variable_lock(key):
            try:
                current = self._value_get(key)
            except KeyError:
                if default is MISSING:
                    raise KeyError(
                        f"Variable {name!r} is not set and no default was given."
                    ) from None
                current = default
            new = fn(current)
            self._value_set(key, new)
        return new

    def delete_variable(self, name: str) -> bool:
        "Deletes the shared variable `name`. Returns True if it existed."
        return self._value_delete(self._key(VARIABLES, name))

    def variables(self) -> list[str]:
        "Returns the names of every shared variable, sorted."
        prefix = self._key(VARIABLES) + self.sep
        return sorted(key.removeprefix(prefix) for key in self._value_keys(prefix))

    # ---- heartbeats ----------------------------------------------------------

    # Implementation notes.
    #
    # Heartbeats reuse the value store, under their own key family, so no
    # transport has to implement anything new: `MemoryConnector`, the fake
    # Redis and both real connectors get them for free, and `clean_namespace`
    # already deletes them (it clears the whole store).
    #
    # The stored value is the writer's `time.time()` and the reader compares
    # it with its own clock. A Redis key with `EXPIRE` would be simpler on
    # Redis, but the filesystem has no expiry and the contract puts the
    # behaviour in this base class, over primitives. The price is a few
    # seconds of clock skew between machines, absorbed by `max_age`.
    #
    # `heartbeats()` tolerates a key deleted between `_value_keys` and
    # `_value_get`: a worker may stop cleanly while a client is reading.
    #
    # The interval travels with every beat, in its own key family. A
    # reader cannot know how often a worker beats, and a reader with a
    # short `max_age` would call a slow worker dead. With the interval
    # published, `dead_workers` gives each worker its own tolerance. Two
    # writes per beat cost nothing at the default 10 s, and the key comes
    # back by itself after a `clean_namespace`. A separate value key, not
    # a dict in the heartbeat key, keeps the float that older readers
    # expect.
    def heartbeat(self, worker_id: str, interval: float | None = None) -> None:
        """Records that `worker_id` is alive now.

        Called periodically by `Worker` from its heartbeat thread. The value
        stored is the writer's `time.time()`.

        Args:
            worker_id (str): Id of the worker.
            interval (float, optional): Seconds between the worker's
                heartbeats. If given, it is stored too, and readers use
                `HEARTBEAT_TOLERANCE` times this value as the tolerance for
                this worker when it is longer than their `max_age`.
                Defaults to None (not stored).

        """
        self._value_set(self._key(HEARTBEATS, worker_id), time.time())
        if interval is not None:
            self._value_set(self._key(HEARTBEAT_INTERVALS, worker_id), interval)

    def _values_by_worker(self, family: str) -> dict:
        prefix = self._key(family) + self.sep
        out = {}
        for key in self._value_keys(prefix):
            try:
                out[key.removeprefix(prefix)] = self._value_get(key)
            except KeyError:
                pass  # deleted meanwhile (clean shutdown of that worker)
        return out

    def heartbeats(self) -> dict:
        """Returns the last heartbeat time of every worker that has one.

        Returns:
            dict: `{worker_id: time}`, with `time` as returned by
                `time.time()` on the worker's machine.

        """
        return self._values_by_worker(HEARTBEATS)

    def heartbeat_intervals(self) -> dict:
        """Returns the heartbeat interval of every worker that published one.

        Returns:
            dict: `{worker_id: seconds}`.

        """
        return self._values_by_worker(HEARTBEAT_INTERVALS)

    def dead_workers(self, max_age: float | None = None) -> dict:
        """Returns the workers whose heartbeat is too old, with their deadline.

        The deadline of a worker is its last heartbeat plus its tolerance.
        The tolerance is `max_age`, or `HEARTBEAT_TOLERANCE` times the
        worker's own interval if that is longer. A worker is dead when the
        reader's clock is past its deadline. Workers without a heartbeat
        key are never in the result.

        Args:
            max_age (float, optional): Seconds. The floor of the tolerance.
                Defaults to None: the connector's
                `default_heartbeat_max_age` (30 s; 61 s on the filesystem).

        Returns:
            dict: `{worker_id: deadline}`, with `deadline` as a
                `time.time()` value.

        """
        if max_age is None:
            max_age = self.default_heartbeat_max_age
        now = time.time()
        intervals = self.heartbeat_intervals()
        out = {}
        for worker_id, beat in self.heartbeats().items():
            tolerance = max(max_age, HEARTBEAT_TOLERANCE * intervals.get(worker_id, 0))
            if now > beat + tolerance:
                out[worker_id] = beat + tolerance
        return out

    def alive_workers(self, max_age: float | None = None) -> set:
        """Returns the ids of the workers with a recent heartbeat.

        Args:
            max_age (float, optional): Seconds. The floor of the tolerance,
                see `dead_workers`. Defaults to None: the connector's
                `default_heartbeat_max_age`.

        Returns:
            set: Worker ids with a heartbeat that is not too old. Workers
                without a heartbeat key are not included: see
                `prune_dead_workers` for how they are treated.

        """
        return set(self.heartbeats()) - set(self.dead_workers(max_age))

    def delete_heartbeat(self, worker_id: str) -> bool:
        "Deletes the heartbeat and interval of `worker_id`. Returns True if the heartbeat existed."
        existed = self._value_delete(self._key(HEARTBEATS, worker_id))
        self._value_delete(self._key(HEARTBEAT_INTERVALS, worker_id))
        return existed

    def prune_dead_workers(self, max_age: float | None = None) -> list:
        """Unregisters the workers whose heartbeat is too old.

        Only workers **with** a stale heartbeat key are pruned: a worker
        that dies never deletes its key. Registered workers without a key
        (older versions, heartbeats disabled, `run_once` only) are left as
        they are. The heartbeat keys of each pruned worker are deleted too.

        Args:
            max_age (float, optional): Seconds. The floor of the tolerance,
                see `dead_workers`. Defaults to None: the connector's
                `default_heartbeat_max_age`.

        Returns:
            list: Sorted ids of the pruned workers.

        """
        dead = sorted(self.dead_workers(max_age))
        for worker_id in dead:
            self.unregister_methods(worker_id)
            self.delete_heartbeat(worker_id)
            logger.info(f"Worker {worker_id} pruned: its heartbeat is too old.")
        return dead
