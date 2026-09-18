import pickle

import pytest

pytest.importorskip("redis")


class FakeRedis:
    """Minimal in-memory stand-in for the redis commands used by the connector.

    Sets for the registry (deleted as soon as they become empty, like redis)
    and lists for the queues. Names come back as bytes, as with
    `decode_responses=False`. `blpop` never blocks: it returns None when the
    queues are empty.
    """

    def __init__(self):
        self.sets = {}
        self.lists = {}

    @staticmethod
    def _key(key):
        return key.decode("utf8") if isinstance(key, bytes) else key

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

    def exists(self, key):
        return 1 if self._key(key) in self.sets else 0

    def scan_iter(self, pattern):
        prefix = pattern[:-1]  # patterns are always "{prefix}*"
        for key in list(self.sets):
            if key.startswith(prefix):
                yield key.encode("utf8")

    def smembers(self, key):
        return {m.encode("utf8") for m in self.sets.get(self._key(key), set())}

    # --- lists (queues) ---
    def rpush(self, key, *values):
        lst = self.lists.setdefault(self._key(key), [])
        lst.extend(values)
        return len(lst)

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

    def delete(self, *keys):
        return sum(self.lists.pop(self._key(k), None) is not None for k in keys)

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


def fn():
    return None


class TestUnregisterMethods:
    def register_two_workers(self, connector):
        q1 = connector.get_requests_queue("q1")
        q2 = connector.get_requests_queue("q2")
        connector.register_methods({q1: {"add": fn, "mul": fn}}, "w1")
        connector.register_methods({q1: {"add": fn}, q2: {"add": fn}}, "w2")
        return q1, q2

    def test_keeps_queues_with_remaining_workers(self, connector):
        q1, q2 = self.register_two_workers(connector)

        connector.unregister_methods("w1")

        assert connector.workers_registry() == {"q1": ["w2"], "q2": ["w2"]}
        assert sorted(connector.all_queues_for_method("add")) == [q1, q2]
        # mul stays available: q1 still has a worker (same coarse-grained
        # semantics as FileSystemConnector.unregister_methods).
        assert connector.all_queues_for_method("mul") == [q1]

    def test_removes_empty_queues_and_methods(self, connector):
        self.register_two_workers(connector)

        connector.unregister_methods("w1")
        connector.unregister_methods("w2")

        assert connector.workers_registry() == {}
        assert connector.methods_registry() == {}
        assert connector.random_queue_for_method("add") is None

    def test_unknown_worker_is_noop(self, connector):
        q1, q2 = self.register_two_workers(connector)

        connector.unregister_methods("other")

        registry = connector.workers_registry()
        assert sorted(registry["q1"]) == ["w1", "w2"]
        assert registry["q2"] == ["w2"]
        assert sorted(connector.all_queues_for_method("add")) == [q1, q2]


class TestQueues:
    def test_messages_are_stored_encoded(self, connector):
        connector.enqueue("q", {"a": [1, 2]})
        assert connector.connection.lists["q"] == [b'{"a": [1, 2]}']

    def test_pop_returns_queue_name_and_object(self, connector):
        connector.enqueue("q", {"a": [1, 2]})
        assert connector.pop("q", timeout=0.1) == ("q", {"a": [1, 2]})
        assert connector.pop("q", timeout=0.1) is None

    def test_pop_multiple_respects_queue_order(self, connector):
        connector.enqueue("low", "second")
        connector.enqueue("high", "first")
        queues = ["high", "low"]
        assert connector.pop_multiple(queues, timeout=0.1) == ("high", "first")
        assert connector.pop_multiple(queues, timeout=0.1) == ("low", "second")
        assert connector.pop_multiple(queues, timeout=0.1) is None

    def test_pop_all_empties_the_queue_in_order(self, connector):
        for i in range(3):
            connector.enqueue("q", i)
        assert connector.pop_all("q") == [0, 1, 2]
        assert connector.pop_all("q") == []

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

    def test_pop_all_skips_undecodable_messages(self, connector, caplog):
        connector.enqueue("q", 1)
        connector.connection.rpush("q", b"garbage")
        connector.enqueue("q", 2)

        with caplog.at_level("ERROR", logger="distributed_processing.redis_connector"):
            assert connector.pop_all("q") == [1, 2]
        assert sum("could not be decoded" in m for m in caplog.messages) == 1
