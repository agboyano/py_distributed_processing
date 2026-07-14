from __future__ import annotations

import logging
import random
import time
from typing import Any

import fs_structs


def sleep(a: float, b: float | None = None) -> None:
    "Sleeps `a` seconds, or a uniform random time in [a, b] if `b` is given."
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


logger = logging.getLogger(__name__)


class FileSystemConnector:
    """Transport on a shared directory (NFS, local disk, ...) via `fs_structs`.

    Queues are `fs_structs` lists and the registry is a `fs_structs` dict,
    all under an `FSNamespace` rooted at `base_path`. Blocking pops wait for
    filesystem events (watchdog) or poll, depending on `with_watchdog`.

    Args:
        base_path (str): Directory shared by clients and workers.
        temp_dir (str, optional): Temporary directory used by the
            namespace for atomic writes. Defaults to None.
        serializer: `fs_structs` serializer used to store the values.
            Defaults to `fs_structs.structs.joblib_serializer`.

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
        lock_pop_timeout (float): Seconds to wait for the queue lock in
            `pop_multiple`.
        lock_pop_watchdog_timeout (float): Seconds to wait for a file event
            while waiting for the queue lock.
        lock_pop_wait (tuple): (min, max) seconds of the uniform random
            wait between queue lock attempts.
        registry_timeout (float): Seconds to wait for registry reads.
        lock_registry_timeout (float): Seconds to wait for the registry lock.
        lock_registry_watchdog_timeout (float): Seconds to wait for a file
            event while waiting for the registry lock.
        lock_registry_wait (tuple): (min, max) seconds of the uniform
            random wait between registry lock attempts.

    """

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

        self.lock_pop_timeout: float = 10
        self.lock_pop_watchdog_timeout: float = 2
        self.lock_pop_wait = (0.0, 0.1)

        self.registry_timeout: float = 10

        self.lock_registry_timeout: float = 60
        self.lock_registry_watchdog_timeout: float = 10
        self.lock_registry_wait = (0.0, 0.1)

    def clean_namespace(self) -> None:
        "Deletes every object linked to the namespace."
        self.namespace.clear()
        self.registry = self.namespace.udict("registry")

    def get_requests_queue(self, queue_name: str) -> str:
        "Returns the requests queue reference for a simple queue name."
        return f"requests_{queue_name}"

    def requests_queue_name(self, queue_ref: str) -> str:
        "Returns the simple queue name for a requests queue reference."
        return queue_ref.removeprefix("requests_")

    def get_responses_queue(self, client_id: str) -> str:
        "Returns the responses queue reference for a client id."
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str: str) -> str:
        "Derives the responses queue from a request id ({client_id}:{n})."
        return id_str.split(":")[0] + "_responses"

    def get_client_id(self) -> str:
        "Generates a new unique client id (locked counter in the registry)."
        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "nclients_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            nclients = self.registry.get("nclients", 0) + 1
            self.registry["nclients"] = nclients
        return f"fs_client_{nclients}"

    def get_server_id(self) -> str:
        "Generates a new unique worker id (locked counter in the registry)."
        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "nservers_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            nservers = self.registry.get("nservers", 0) + 1
            self.registry["nservers"] = nservers
        return f"fs_server_{nservers}"

    def methods_registry(self) -> dict:
        """Returns {method: [queue_ref, ...]}. Used by clients."""
        registry = {}

        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "registry_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            method_queues = [x for x in self.registry.keys() if "method_queues_" in x]

            for method_set in method_queues:
                method = method_set.replace("method_queues_", "")
                available = [x for x in self.registry[method_set]]
                registry[method] = available

        return registry

    def workers_registry(self) -> dict:
        """Returns {queue_ref: [worker_id, ...]}. Used by clients."""
        registry = {}

        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "registry_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            workers_queues = [x for x in self.registry.keys() if "workers_queue_" in x]

            for worker_queues in workers_queues:
                worker = worker_queues.replace("workers_queue_", "")
                available = [x for x in self.registry[worker_queues]]
                registry[worker] = available

        return registry

    def register_methods(self, requests_queues_dict: dict, worker_id: str) -> None:
        """Registers worker's public functions and their associated FIFO queues.

        Args:
            requests_queues_dict: A dictionary mapping queue names to method
                                  dictionaries, where each method dictionary has
                                  function names as keys and callable functions as values.

                                  {'queue_name': {'function_name': callable_function, ...}, ... }
            worker_id: Id of the worker publishing the methods.

        Client Configuration Options:
            - check_registry='cache' (default): Clients cache the registry and can
              refresh it with update_registry_cache().
            - check_registry='always': Client always checks the latest registry
              before dispatching. Huge overhead.
            - Any other value: Clients ignore the registry and send requests to
              their default queue (see set_default_queue()).
        """
        registry = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                registry[method] = registry.get(method, []) + [queue_name]

        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "registry_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            for method in registry:
                method_set = f"method_queues_{method}"
                tmp = self.registry.get(method_set, set())
                self.registry[method_set] = tmp.union(registry[method])
                queues = ", ".join(str(q) for q in registry[method])
                logger.info(
                    f"Method {method} published as available for queues: {queues}"
                )

            for queue_name in requests_queues_dict:
                queue_set = f"workers_queue_{queue_name}"
                tmp = self.registry.get(queue_set, set())
                self.registry[queue_set] = tmp.union({worker_id})

    def _unregister_member_from_sets(
        self, prefix, member, settype="Method", listeners="queues"
    ):
        deleted_variables = []
        for s in [x for x in self.registry.keys() if prefix in x]:
            tmp = self.registry[s]
            tmp.discard(member)
            name = s.removeprefix(prefix)
            if len(tmp) == 0:
                del self.registry[s]
                logger.info(
                    f"{settype} {name} has not remaining public {listeners} listening. {member} unregistered."
                )
                deleted_variables.append(name)
            else:
                self.registry[s] = tmp
        return deleted_variables

    def unregister_methods(self, worker_id: str) -> None:
        """Removes a worker from the registry.

        Queues without remaining workers, and methods without remaining
        queues, are deleted from the registry.
        """
        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "registry_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            for queue_name in self._unregister_member_from_sets(
                "workers_queue_", worker_id, "Queue", "workers"
            ):
                _ = self._unregister_member_from_sets(
                    "method_queues_", queue_name, "Method", "queues"
                )

    def random_queue_for_method(self, method: str) -> str | None:
        "Returns a random queue ref serving `method`, or None if there is none."
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method: str) -> list:
        "Returns all the queue refs where requests for `method` can be sent."
        method_set = f"method_queues_{method}"
        return list(self.registry.get(method_set, []))

    def enqueue(self, queue_name: str, msg: Any) -> None:
        "Appends a serialized message to the queue."
        queue = self.namespace.list(queue_name)
        queue.append(msg)

    def pop(self, queue_name: str, timeout: float = -1) -> tuple | None:
        """Blocking pop operation for retrieving first item from a FIFO queue.

        Args:
            queue_name: Name of the queue to pop first item from
            timeout: Maximum time to wait in seconds (< 0 = waits indefinitely, 0 = try once)


        Returns:
            tuple: (queue_name, value) if first item found, None if timeout occurs

        Note:
            - Used by clients
            - Supports both watchdog and polling modes
        """

        def try_pop(queue_name, queue):
            try:
                # using pop(0) instead of pop_left. Expected only one client per results queue.
                return (queue_name, queue.pop(0))
            except (IndexError, KeyError):
                return False

        queue = self.namespace.list(queue_name)

        if ok := try_pop(queue_name, queue):
            return ok

        start_time = time.time()
        time_left = timeout
        wait_forever = True if timeout < -0.001 else False
        while time_left > 0.0 or wait_forever:
            new_watchdog_timeout = (
                self.pop_watchdog_timeout
                if wait_forever
                else min(self.pop_watchdog_timeout, time_left)
            )
            if self.with_watchdog:
                _ = fs_structs.watchdog.wait_until_file_event(
                    [queue.base_path], [], ["created"], timeout=new_watchdog_timeout
                )
                # Wait random time to minimize probability of race conditions
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)  # Standard polling delay

            if ok := try_pop(queue_name, queue):
                return ok

            time_left = timeout - (time.time() - start_time)
        return None  # Timeout reached

    def pop_multiple(self, queue_names: list, timeout: float = -1) -> tuple | None:
        """Blocking pop(0) from multiple FIFO queues in priority order (highest first).

        Args:
            queue_names: List of queue names (ordered by priority - highest first)
            timeout: Maximum wait time in seconds (< 0 = waits indefinitely, 0 = try once)

        Returns:
            tuple: (queue_name, value) if item found, None if timeout reached

        Note:
            - Used by workers
            - Checks queues in order until item is found
            - Supports both watchdog and polling modes
        """

        def try_pop_multiple(
            queue_refs,
            timeout=self.lock_pop_timeout,
            watchdog_timeout=self.lock_pop_watchdog_timeout,
            wait=self.lock_pop_wait,
        ):
            for q_name, queue in queue_refs:
                try:
                    return (q_name, queue.pop_left(timeout, watchdog_timeout, wait))
                except (IndexError, KeyError):
                    continue
            return False

        queue_refs = [(q, self.namespace.list(q)) for q in queue_names]

        # Walrus operator, introduced in Python 3.8
        if ok := try_pop_multiple(queue_refs):
            return ok

        watch_paths = [q[1].base_path for q in queue_refs]

        start_time = time.time()
        time_left = timeout
        wait_forever = True if timeout < -0.001 else False
        while time_left > 0.0 or wait_forever:
            new_watchdog_timeout = (
                self.pop_watchdog_timeout
                if wait_forever
                else min(self.pop_watchdog_timeout, time_left)
            )

            if self.with_watchdog:
                _ = fs_structs.watchdog.wait_until_file_event(
                    watch_paths,
                    [],
                    ["created"],
                    timeout=new_watchdog_timeout,
                )
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)
            # Walrus operator, introduced in Python 3.8
            if ok := try_pop_multiple(queue_refs):
                return ok

            time_left = timeout - (time.time() - start_time)

        return None  # Timeout expired

    def pop_all(self, queue_name: str) -> list:
        """Pops all available messages in the queue named queue_name (in order).

        Used by clients.
        """
        queue = self.namespace.list(queue_name)
        N = len(queue)
        # using pop(0) instead of pop_left. Expected only one client per results queue
        return [queue.pop(0) for _ in range(N)]
