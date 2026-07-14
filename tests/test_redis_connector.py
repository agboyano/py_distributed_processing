import pytest

pytest.importorskip("redis")


class FakeRedis:
    """Minimal in-memory stand-in for the redis set commands used by the registry.

    Like redis, it deletes a set as soon as it becomes empty.
    """

    def __init__(self):
        self.sets = {}

    @staticmethod
    def _key(key):
        return key.decode("utf8") if isinstance(key, bytes) else key

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


@pytest.fixture
def connector():
    from distributed_processing.redis_connector import RedisConnector

    # redis.Redis does not connect until the first command, so building
    # the connector is safe; the connection is then replaced by the fake.
    c = RedisConnector("localhost")
    c.connection = FakeRedis()
    return c


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
