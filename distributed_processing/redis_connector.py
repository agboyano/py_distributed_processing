from __future__ import annotations

import logging
import random
from typing import Any

import redis

logger = logging.getLogger(__name__)


class RedisConnector:
    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        namespace: str = "tasks",
    ):
        self.connection = redis.Redis(redis_host, redis_port, redis_db)
        self.namespace = namespace

    def clean_namespace(self) -> None:
        "Deletes every object linked to the namespace."
        for item in self.connection.scan_iter(f"{self.namespace}:*"):
            self.connection.delete(item)

    def get_requests_queue(self, queue_name: str) -> str:
        return f"{self.namespace}:requests:{queue_name}"

    def requests_queue_name(self, queue_ref: str) -> str:
        return queue_ref.split(":")[-1]

    def get_client_id(self) -> str:
        nclients = str(self.connection.incr(f"{self.namespace}:nclients", 1))
        return f"{self.namespace}:redis_client:{nclients}"

    def get_server_id(self) -> str:
        nservers = str(self.connection.incr(f"{self.namespace}:nservers", 1))
        return f"{self.namespace}:redis_server:{nservers}"

    def get_responses_queue(self, client_id: str) -> str:
        return f"{client_id}:responses"

    def get_reply_to_from_id(self, id_str: str) -> str:
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

    def random_queue_for_method(self, method: str) -> str | None:
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method: str) -> list:
        method_set = f"{self.namespace}:method_queues:{method}"
        return [x.decode("utf8") for x in self.connection.smembers(method_set)]

    def enqueue(self, queue: str, msg: Any) -> None:
        self.connection.rpush(queue, msg)

    def pop(self, queue: str, timeout: float = -1) -> tuple | None:
        """Blocking pop. timeout <= 0 waits indefinitely. Used by clients."""
        # blpop timeout == 0 waits indefinitely
        return self.connection.blpop(queue, timeout=max(timeout, 0))

    def pop_multiple(self, queues: list, timeout: float = -1) -> tuple | None:
        """Blocking pop from multiple queues, ordered by priority (highest first).

        timeout: maximum wait time in seconds (<= 0 = wait indefinitely).
        Returns (queue_name, value), or None on timeout. Used by workers.
        """
        # blpop timeout == 0 waits indefinitely
        request_redis = self.connection.blpop(queues, timeout=max(timeout, 0))
        if request_redis is not None:
            return request_redis[0].decode("utf8"), request_redis[1]

        return None

    def pop_all(self, queue: str) -> list:
        """Pops and returns every message available in the queue. Used by clients."""
        pipe = self.connection.pipeline()
        pipe.lrange(queue, 0, -1)
        pipe.delete(queue)
        return pipe.execute()[0]
