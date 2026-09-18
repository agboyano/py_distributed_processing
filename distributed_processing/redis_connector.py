from __future__ import annotations

import logging
import random
import time
from typing import Any

import redis

from .serializers import JsonSerializer

logger = logging.getLogger(__name__)


class RedisConnector:
    """Transport on Redis: lists as queues, sets as registries.

    All keys are prefixed with the namespace, so several independent
    deployments can share the same Redis database.

    Messages are handed to `enqueue` as Python objects and returned by the
    `pop*` methods as Python objects: the connector encodes them with
    `serializer` before storing them in Redis and decodes them on the way
    out. The connection is created with `decode_responses=False` (the
    redis-py default) so payloads reach the serializer as raw bytes; the
    connector decodes key and queue names itself. `decode_responses=True`
    would break every serializer whose output is not UTF-8 text (pickle,
    msgpack...).

    Args:
        redis_host (str): Redis server host. Defaults to 'localhost'.
        redis_port (int): Redis server port. Defaults to 6379.
        redis_db (int): Redis database number. Defaults to 0.
        namespace (str): Prefix for every key used by the connector.
            Defaults to 'tasks'.
        serializer (optional): Object with `dumps(obj) -> bytes` and
            `loads(bytes) -> obj`. Defaults to `JsonSerializer()`. Any
            module with that pair works as is (`pickle`, `dill`, `msgpack`).
            A message that `loads` cannot decode is logged and skipped.

    """

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        namespace: str = "tasks",
        serializer=None,
    ):
        self.connection = redis.Redis(
            redis_host, redis_port, redis_db, decode_responses=False
        )
        self.namespace = namespace
        self.serializer = JsonSerializer() if serializer is None else serializer

    def clean_namespace(self) -> None:
        "Deletes every object linked to the namespace."
        for item in self.connection.scan_iter(f"{self.namespace}:*"):
            self.connection.delete(item)

    def get_requests_queue(self, queue_name: str) -> str:
        "Returns the requests queue reference for a simple queue name."
        return f"{self.namespace}:requests:{queue_name}"

    def requests_queue_name(self, queue_ref: str) -> str:
        "Returns the simple queue name for a requests queue reference."
        return queue_ref.split(":")[-1]

    def get_client_id(self) -> str:
        "Generates a new unique client id (atomic counter in Redis)."
        nclients = str(self.connection.incr(f"{self.namespace}:nclients", 1))
        return f"{self.namespace}:redis_client:{nclients}"

    def get_server_id(self) -> str:
        "Generates a new unique worker id (atomic counter in Redis)."
        nservers = str(self.connection.incr(f"{self.namespace}:nservers", 1))
        return f"{self.namespace}:redis_server:{nservers}"

    def get_responses_queue(self, client_id: str) -> str:
        "Returns the responses queue reference for a client id."
        return f"{client_id}:responses"

    def get_reply_to_from_id(self, id_str: str) -> str:
        "Derives the responses queue from a request id ({client_id}:{n})."
        return ":".join(id_str.split(":")[:-1]) + ":responses"

    def methods_registry(self) -> dict:
        """Returns {method: [queue_ref, ...]}. Used by clients.

        Each method has a redis set with key {namespace}:method_queues:{method}
        whose members are the queues where requests for that method can be sent.
        """
        registry = {}
        for method_set in self.connection.scan_iter(
            f"{self.namespace}:method_queues:*"
        ):
            method = method_set.decode("utf8").split(":")[-1]
            available = [x.decode("utf8") for x in self.connection.smembers(method_set)]
            registry[method] = available

        return registry

    def workers_registry(self) -> dict:
        """Returns {queue_ref: [worker_id, ...]}. Used by clients.

        Each queue has a redis set with key {namespace}:workers_queue:{queue}
        whose members are the workers listening on that queue.
        """
        registry = {}
        for method_set in self.connection.scan_iter(
            f"{self.namespace}:workers_queue:*"
        ):
            method = method_set.decode("utf8").split(":")[-1]
            available = [x.decode("utf8") for x in self.connection.smembers(method_set)]
            registry[method] = available

        return registry

    def register_methods(self, requests_queues_dict: dict, worker_id: str) -> None:
        """Used by workers to publish their methods and queues.

        requests_queues_dict maps queue names to dicts with method names
        as keys and functions as values: {queue: {method_name: fn, ...}, ...}.
        """
        registry: dict = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                if method in registry:
                    registry[method] += [queue_name]
                else:
                    registry[method] = [queue_name]

        for method in registry:
            method_set = f"{self.namespace}:method_queues:{method}"
            self.connection.sadd(method_set, *registry[method])
            queues = ", ".join(str(q) for q in registry[method])
            logger.info(f"Method {method} published as available for queues: {queues}")

        for queue_name in requests_queues_dict:
            queue_set = f"{self.namespace}:workers_queue:{queue_name}"
            self.connection.sadd(queue_set, worker_id)

    def unregister_methods(self, worker_id: str) -> None:
        """Removes a worker from the registry.

        Queues without remaining workers, and methods without remaining
        queues, are deleted from the registry (redis deletes empty sets
        automatically).
        """
        empty_queues = []
        for queue_set in self.connection.scan_iter(f"{self.namespace}:workers_queue:*"):
            removed = self.connection.srem(queue_set, worker_id)
            if removed and not self.connection.exists(queue_set):
                queue_ref = queue_set.decode("utf8").removeprefix(
                    f"{self.namespace}:workers_queue:"
                )
                empty_queues.append(queue_ref)
                logger.info(
                    f"Queue {queue_ref} has no remaining public workers listening. {worker_id} unregistered."
                )

        if len(empty_queues) == 0:
            return

        for method_set in self.connection.scan_iter(
            f"{self.namespace}:method_queues:*"
        ):
            removed = self.connection.srem(method_set, *empty_queues)
            if removed and not self.connection.exists(method_set):
                method = method_set.decode("utf8").split(":")[-1]
                logger.info(
                    f"Method {method} has no remaining public queues listening. Unregistered."
                )

    def random_queue_for_method(self, method: str) -> str | None:
        "Returns a random queue ref serving `method`, or None if there is none."
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method: str) -> list:
        "Returns all the queue refs where requests for `method` can be sent."
        method_set = f"{self.namespace}:method_queues:{method}"
        return [x.decode("utf8") for x in self.connection.smembers(method_set)]

    def _loads(self, queue: bytes | str, raw: bytes) -> tuple | None:
        "Returns (queue_name, obj), or None (after logging) if `raw` cannot be decoded."
        queue_name = queue.decode("utf8") if isinstance(queue, bytes) else queue
        try:
            return queue_name, self.serializer.loads(raw)
        except Exception:
            logger.error(
                f"Message from queue {queue_name} could not be decoded and was dropped: {raw[:80]!r}"
            )
            return None

    def _blpop(self, queues: str | list, timeout: float) -> tuple | None:
        """Blocking pop from one or more queues, skipping undecodable messages.

        timeout <= 0 waits indefinitely. Returns (queue_name, obj), or None
        on timeout. A skipped message does not extend the total wait.
        """
        forever = timeout <= 0
        deadline = None if forever else time.time() + timeout
        while True:
            # blpop timeout == 0 waits indefinitely
            wait = 0 if forever else deadline - time.time()
            if not forever and wait <= 0:
                return None
            popped = self.connection.blpop(queues, timeout=wait)
            if popped is None:
                return None
            decoded = self._loads(*popped)
            if decoded is not None:
                return decoded

    def enqueue(self, queue: str, msg: Any) -> None:
        "Appends a message to the queue (encoded with the serializer)."
        self.connection.rpush(queue, self.serializer.dumps(msg))

    def pop(self, queue: str, timeout: float = -1) -> tuple | None:
        """Blocking pop. timeout <= 0 waits indefinitely. Used by clients.

        Returns (queue_name, obj), or None on timeout.
        """
        return self._blpop(queue, timeout)

    def pop_multiple(self, queues: list, timeout: float = -1) -> tuple | None:
        """Blocking pop from multiple queues, ordered by priority (highest first).

        timeout: maximum wait time in seconds (<= 0 = wait indefinitely).
        Returns (queue_name, obj), or None on timeout. Used by workers.
        """
        return self._blpop(queues, timeout)

    def pop_all(self, queue: str) -> list:
        """Pops and returns every message available in the queue. Used by clients.

        Undecodable messages are logged and left out.
        """
        pipe = self.connection.pipeline()
        pipe.lrange(queue, 0, -1)
        pipe.delete(queue)
        raw_messages = pipe.execute()[0]
        decoded = [self._loads(queue, raw) for raw in raw_messages]
        return [d[1] for d in decoded if d is not None]
