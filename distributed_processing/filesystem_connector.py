import logging
import random
import time

import fs_structs


def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


logger = logging.getLogger(__name__)


class FileSystemConnector(object):
    def __init__(
        self, base_path, temp_dir=None, serializer=fs_structs.structs.joblib_serializer
    ):
        self.namespace = fs_structs.structs.FSNamespace(base_path, temp_dir, serializer)
        self.registry = self.namespace.udict("registry")
        self.with_watchdog = True

        self.pop_sleep = (5, 10)  # sólo si with_watchdog es False

        self.pop_timeout = 60
        self.pop_watchdog_timeout = 60
        self.pop_sleep_watchdog = (0.0, 0.1)

        self.lock_pop_timeout = 10
        self.lock_pop_watchdog_timeout = 2
        self.lock_pop_wait = (0.0, 0.1)

        self.registry_timeout = 10

        self.lock_registry_timeout = 60
        self.lock_registry_watchdog_timeout = 10
        self.lock_registry_wait = (0.0, 0.1)

    def clean_namespace(self):
        "Borra todos los objetos vinculados al namespace"
        self.namespace.clear()
        self.registry = self.namespace.udict("registry")

    def get_requests_queue(self, queue_name):
        return f"requests_{queue_name}"

    def get_responses_queue(self, client_id):
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str):
        return id_str.split(":")[0] + "_responses"

    def get_client_id(self):
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

    def get_server_id(self):
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

    def methods_registry(self):
        """
        Lo usa el cliente.
        Cada método tiene un set de redis con clave {namespace}:method_queues:{method}.
        El contenido del set son los nombres de las colas donde se pueden enviar
        los request para ejecutar ese método.
        """
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

    def register_methods(self, requests_queues_dict, worker_id):
        """Registers worker's public functions and their associated FIFO queues.

        Args:
            requests_queues_dict: A dictionary mapping queue names to method
                                  dictionaries, where each method dictionary has
                                  function names as keys and callable functions as values.

                                  {'queue_name': {'function_name': callable_function, ...}, ... }

        Client Configuration Options:
            - check_registry='never': Clients must manually specify queues with set_default_queue().
            - check_registry='cache' (default): Clients can update the cache with update_registry().
            - check_registry='always': Client always checks the latest registry before dispatching.
                                       Huge overhead.

        Note:
            - The Client method select_queue() selects the queue to use.
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
                colas = ", ".join(str(q) for q in registry[method])
                logger.info(
                    f"Method {method} published as available for queues: {colas}"
                )
            
            for queue_name in requests_queues_dict:
                queue_set = f"workers_queue_{queue_name}"
                tmp = self.registry.get(queue_set, set())
                self.registry[queue_set] = tmp.union({worker_id})


    def _unregister_member_from_sets(self, prefix, member, settype ="Method", listeners="queues"):
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

    def unregister_methods(self, worker_id):
        with fs_structs.structs.lock_context(
            self.registry.base_path,
            "registry_lock",
            self.lock_registry_timeout,
            self.lock_registry_watchdog_timeout,
            self.lock_registry_wait,
        ):
            for queue_name in self._unregister_member_from_sets("workers_queue_", worker_id, "Queue", "workers"):
                _ = self._unregister_member_from_sets("method_queues_", queue_name, "Method", "queues")

    def random_queue_for_method(self, method):
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method):
        method_set = f"method_queues_{method}"
        return [x for x in self.registry[method_set]]

    def enqueue(self, queue_name, msg):
        queue = self.namespace.list(queue_name)
        queue.append(msg)

    def pop(self, queue_name, timeout=-1):
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

    def pop_multiple(self, queue_names, timeout=-1):
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

        if self.with_watchdog:
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

    def pop_all(self, queue_name):
        """Pops all available messages in the queue named queue_name (in order).

        Used by clients.
        """
        queue = self.namespace.list(queue_name)
        N = len(queue)
        # using pop(0) instead of pop_left. Expected only one client per results queue
        return [queue.pop(0) for i in range(N)]
