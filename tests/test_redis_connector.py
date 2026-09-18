"""Redis-specific behaviour of `RedisConnector` on a fake in-memory server.

The contract shared with the other connectors (names, registry, queue
semantics) is checked in `test_connector.py`.
"""

import pickle
import threading

import pytest

pytest.importorskip("redis")


class FakeRedis:
    """Minimal in-memory stand-in for the redis commands used by the connector.

    Sets for the registry (deleted as soon as they become empty, like redis),
    lists for the queues and plain integers for the counters. Names come
    back as bytes, as with `decode_responses=False`. `blpop` never blocks:
    it returns None when the queues are empty.
    """

    def __init__(self):
        self.sets = {}
        self.lists = {}
        self.counters = {}
        self.strings = {}
        self.locks = {}

    @property
    def _stores(self):
        return (self.sets, self.lists, self.counters, self.strings)

    @staticmethod
    def _key(key):
        return key.decode("utf8") if isinstance(key, bytes) else key

    # --- keys ---
    def scan_iter(self, pattern):
        prefix = pattern[:-1]  # patterns are always "{prefix}*"
        for store in self._stores:
            for key in list(store):
                if key.startswith(prefix):
                    yield key.encode("utf8")

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            for store in self._stores:
                deleted += store.pop(self._key(key), None) is not None
        return deleted

    def exists(self, key):
        key = self._key(key)
        return int(any(key in store for store in self._stores))

    # --- locks (update_variable) ---
    def lock(self, name, **kwargs):
        # A threading.Lock stands in for redis.lock.Lock: both are context managers.
        return self.locks.setdefault(self._key(name), threading.Lock())

    # --- strings (variables) ---
    def set(self, key, value):
        self.strings[self._key(key)] = value
        return True

    def get(self, key):
        return self.strings.get(self._key(key))

    # --- counters ---
    def incr(self, key, amount=1):
        key = self._key(key)
        self.counters[key] = self.counters.get(key, 0) + amount
        return self.counters[key]

    # --- sets (registry) ---
    def sadd(self, key, *members):
        s = self.sets.setdefault(self._key(key), set())
        added = len(set(members) - s)
        s.update(members)
        return added

    def srem(self, key, *members):
        key = self._key(key)
        s = self.sets.get(key, set())
        removed = len(s & set(members))
        s.difference_update(members)
        if len(s) == 0:
            self.sets.pop(key, None)
        return removed

    def smembers(self, key):
        return {m.encode("utf8") for m in self.sets.get(self._key(key), set())}

    # --- lists (queues) ---
    def rpush(self, key, *values):
        lst = self.lists.setdefault(self._key(key), [])
        lst.extend(values)
        return len(lst)

    def lpop(self, key):
        lst = self.lists.get(self._key(key))
        return lst.pop(0) if lst else None

    def blpop(self, keys, timeout=0):
        keys = [keys] if isinstance(keys, (str, bytes)) else keys
        for key in keys:
            lst = self.lists.get(self._key(key))
            if lst:
                return self._key(key).encode("utf8"), lst.pop(0)
        return None

    def lrange(self, key, start, end):
        lst = self.lists.get(self._key(key), [])
        return list(lst[start:] if end == -1 else lst[start : end + 1])

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    "Records the calls and replays them on `execute`."

    def __init__(self, fake):
        self._fake = fake
        self._calls = []

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self._calls.append((name, args, kwargs))
            return self

        return record

    def execute(self):
        return [getattr(self._fake, n)(*a, **k) for n, a, k in self._calls]


def make_connector(**kwargs):
    from distributed_processing.redis_connector import RedisConnector

    # redis.Redis does not connect until the first command, so building
    # the connector is safe; the connection is then replaced by the fake.
    c = RedisConnector("localhost", **kwargs)
    c.connection = FakeRedis()
    return c


@pytest.fixture
def connector():
    return make_connector()


class TestKeys:
    def test_keys_are_prefixed_with_the_namespace(self, connector):
        assert connector.get_requests_queue("q") == "tasks:requests:q"
        assert connector.get_client_id() == "tasks:redis_client:1"
        assert connector.get_server_id() == "tasks:redis_server:1"
        assert connector.get_responses_queue("tasks:redis_client:1") == (
            "tasks:redis_client:1:responses"
        )
        assert connector.get_reply_to_from_id("tasks:redis_client:1:9") == (
            "tasks:redis_client:1:responses"
        )

    def test_registry_lives_in_namespaced_sets(self, connector):
        q = connector.get_requests_queue("q")
        connector.register_methods({q: {"add": lambda: None}}, "w1")
        assert connector.connection.sets == {
            "tasks:method_queues:add": {q},
            f"tasks:workers_queue:{q}": {"w1"},
        }


class TestQueues:
    def test_messages_are_stored_encoded(self, connector):
        connector.enqueue("q", {"a": [1, 2]})
        assert connector.connection.lists["q"] == [b'{"a": [1, 2]}']

    def test_pop_returns_queue_name_and_object(self, connector):
        connector.enqueue("q", {"a": [1, 2]})
        assert connector.pop("q", timeout=0.1) == ("q", {"a": [1, 2]})
        assert connector.pop("q", timeout=0.1) is None

    def test_any_dumps_loads_pair_works_as_serializer(self):
        # pickle has no JSON limits: tuples and bytes survive the round trip.
        c = make_connector(serializer=pickle)
        msg = (1, {"x": b"\x00\xff"})
        c.enqueue("q", msg)
        assert c.pop("q", timeout=0.1) == ("q", msg)

    def test_undecodable_messages_are_skipped_and_logged(self, connector, caplog):
        connector.connection.rpush("q", b"not json at all")
        connector.enqueue("q", "ok")
        connector.connection.rpush("q", b"\xff")
        connector.enqueue("q", "also ok")

        with caplog.at_level("ERROR", logger="distributed_processing.redis_connector"):
            assert connector.pop("q", timeout=0.1) == ("q", "ok")
            assert connector.pop_multiple(["q"], timeout=0.1) == ("q", "also ok")
        assert sum("could not be decoded" in m for m in caplog.messages) == 2

    def test_zero_timeout_skips_undecodable_messages_too(self, connector, caplog):
        connector.connection.rpush("q", b"garbage")
        connector.enqueue("q", "ok")

        with caplog.at_level("ERROR", logger="distributed_processing.redis_connector"):
            assert connector.pop("q", timeout=0) == ("q", "ok")
        assert sum("could not be decoded" in m for m in caplog.messages) == 1

    def test_pop_all_skips_undecodable_messages(self, connector, caplog):
        connector.enqueue("q", 1)
        connector.connection.rpush("q", b"garbage")
        connector.enqueue("q", 2)

        with caplog.at_level("ERROR", logger="distributed_processing.redis_connector"):
            assert connector.pop_all("q") == [1, 2]
        assert sum("could not be decoded" in m for m in caplog.messages) == 1
