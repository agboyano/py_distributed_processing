from __future__ import annotations

import logging
import time
from collections.abc import Iterable
from typing import Any

import redis

from .connector import Connector
from .serializers import JsonSerializer

logger = logging.getLogger(__name__)


class RedisConnector(Connector):
    """Transport on Redis: lists as queues, sets as registries.

    All keys are prefixed with the namespace, so several independent
    deployments can share the same Redis database. Registry updates need
    no lock: every Redis command is atomic.

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
            `loads(bytes) -> obj`. Defaults to `JsonSerializer()`. See also
            `PickleSerializer` and `JoblibSerializer` in `serializers`; any
            module with that pair works as is (`pickle`, `dill`, `msgpack`).
            A message that `loads` cannot decode is logged and skipped.

    """

    sep = ":"
    id_prefix = "redis"

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
        "Deletes every key of the namespace (queues, registry and counters)."
        for item in self.connection.scan_iter(f"{self.namespace}:*"):
            self.connection.delete(item)

    # ---- primitives ----------------------------------------------------------

    def _key(self, *parts: str) -> str:
        return ":".join((self.namespace, *parts))

    def _incr(self, key: str) -> int:
        return int(self.connection.incr(key, 1))

    def _set_add(self, key: str, members: Iterable[str]) -> None:
        self.connection.sadd(key, *members)

    def _set_discard(self, key: str, members: Iterable[str]) -> int:
        return int(self.connection.srem(key, *members))

    def _set_members(self, key: str) -> set[str]:
        return {m.decode("utf8") for m in self.connection.smembers(key)}

    def _set_keys(self, prefix: str) -> list[str]:
        return [k.decode("utf8") for k in self.connection.scan_iter(f"{prefix}*")]

    def _set_delete(self, key: str) -> None:
        # Redis deletes empty sets by itself; this is a no-op then.
        self.connection.delete(key)

    # ---- queues --------------------------------------------------------------

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
        """Pop from one or more queues, skipping undecodable messages.

        timeout < 0 waits indefinitely, 0 checks once (LPOP), > 0 waits at
        most that long (BLPOP). Returns (queue_name, obj), or None. A
        skipped message does not extend the total wait.
        """
        queues = [queues] if isinstance(queues, str) else list(queues)
        if timeout == 0:
            for queue in queues:
                raw = self.connection.lpop(queue)
                while raw is not None:
                    decoded = self._loads(queue, raw)
                    if decoded is not None:
                        return decoded
                    raw = self.connection.lpop(queue)
            return None

        forever = timeout < 0
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
        """Blocking pop. Used by clients.

        timeout < 0 waits indefinitely, 0 checks once, > 0 waits at most
        that many seconds. Returns (queue_name, obj), or None on timeout.
        """
        return self._blpop(queue, timeout)

    def pop_multiple(self, queues: list, timeout: float = -1) -> tuple | None:
        """Blocking pop from multiple queues, ordered by priority (highest first).

        timeout < 0 waits indefinitely, 0 checks once, > 0 waits at most
        that many seconds. Returns (queue_name, obj), or None on timeout.
        Used by workers.
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
