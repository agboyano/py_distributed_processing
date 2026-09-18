"""Behavioural contract of `Connector`, checked against every implementation.

Each test is one rule of the contract documented in
`distributed_processing/connector.py`. The filesystem connector is an
integration case (real directory); the Redis one runs on `FakeRedis`.
"""

import pytest
from conftest import MemoryConnector

from distributed_processing.connector import Connector


def fn():
    return None


@pytest.fixture(
    params=[
        "memory",
        pytest.param("filesystem", marks=pytest.mark.integration),
        "redis",
    ]
)
def connector(request, tmp_path):
    if request.param == "memory":
        return MemoryConnector()
    if request.param == "filesystem":
        pytest.importorskip("fs_structs")
        from distributed_processing.filesystem_connector import FileSystemConnector

        return FileSystemConnector(str(tmp_path))
    pytest.importorskip("redis")
    from test_redis_connector import make_connector

    return make_connector()


def register_two_workers(connector):
    q1 = connector.get_requests_queue("q1")
    q2 = connector.get_requests_queue("q2")
    connector.register_methods({q1: {"add": fn, "mul": fn}}, "w1")
    connector.register_methods({q1: {"add": fn}, q2: {"add": fn}}, "w2")
    return q1, q2


class TestNames:
    def test_requests_queue_name_round_trip(self, connector):
        ref = connector.get_requests_queue("cola_1")
        assert ref != "cola_1"
        assert connector.requests_queue_name(ref) == "cola_1"

    def test_reply_to_is_the_responses_queue_of_the_client(self, connector):
        client_id = connector.get_client_id()
        request_id = f"{client_id}:7"
        assert connector.get_reply_to_from_id(request_id) == (
            connector.get_responses_queue(client_id)
        )

    def test_ids_are_unique_and_named_after_the_connector(self, connector):
        c1, c2 = connector.get_client_id(), connector.get_client_id()
        s1, s2 = connector.get_server_id(), connector.get_server_id()
        assert c1 != c2 and s1 != s2
        assert f"{connector.id_prefix}_client" in c1
        assert f"{connector.id_prefix}_server" in s1
        assert c1.endswith(f"{connector.sep}1") and c2.endswith(f"{connector.sep}2")


class TestRegistry:
    def test_register_methods_is_idempotent(self, connector):
        q1 = connector.get_requests_queue("q1")
        connector.register_methods({q1: {"add": fn}}, "w1")
        connector.register_methods({q1: {"add": fn}}, "w1")
        assert connector.methods_registry() == {"add": [q1]}
        assert connector.workers_registry() == {q1: ["w1"]}

    def test_registry_snapshots_use_queue_refs(self, connector):
        q1, q2 = register_two_workers(connector)
        methods = connector.methods_registry()
        assert sorted(methods["add"]) == [q1, q2]
        assert methods["mul"] == [q1]
        workers = connector.workers_registry()
        assert sorted(workers[q1]) == ["w1", "w2"]
        assert workers[q2] == ["w2"]

    def test_unregister_keeps_queues_with_remaining_workers(self, connector):
        q1, q2 = register_two_workers(connector)

        connector.unregister_methods("w1")

        assert connector.workers_registry() == {q1: ["w2"], q2: ["w2"]}
        assert sorted(connector.all_queues_for_method("add")) == [q1, q2]
        # Coarse-grained: mul stays available because q1 still has a worker,
        # even though w2 never offered mul.
        assert connector.all_queues_for_method("mul") == [q1]

    def test_unregister_removes_empty_queues_and_methods(self, connector):
        register_two_workers(connector)

        connector.unregister_methods("w1")
        connector.unregister_methods("w2")

        assert connector.workers_registry() == {}
        assert connector.methods_registry() == {}
        assert connector.random_queue_for_method("add") is None

    def test_unregister_unknown_worker_is_noop(self, connector):
        q1, q2 = register_two_workers(connector)

        connector.unregister_methods("other")

        registry = connector.workers_registry()
        assert sorted(registry[q1]) == ["w1", "w2"]
        assert registry[q2] == ["w2"]
        assert sorted(connector.all_queues_for_method("add")) == [q1, q2]

    def test_unknown_method_has_no_queues(self, connector):
        assert connector.all_queues_for_method("nope") == []
        assert connector.random_queue_for_method("nope") is None

    def test_random_queue_serves_the_method(self, connector):
        q1, q2 = register_two_workers(connector)
        for _ in range(10):
            assert connector.random_queue_for_method("add") in (q1, q2)
            assert connector.random_queue_for_method("mul") == q1


class TestQueues:
    def test_pop_returns_queue_ref_and_object(self, connector):
        connector.enqueue("q", {"a": [1, 2]})
        assert connector.pop("q", timeout=0) == ("q", {"a": [1, 2]})

    def test_pop_with_zero_timeout_returns_none_without_waiting(self, connector):
        assert connector.pop("empty", timeout=0) is None
        assert connector.pop_multiple(["empty", "also_empty"], timeout=0) is None

    def test_pop_multiple_respects_priority_order(self, connector):
        connector.enqueue("low", "second")
        connector.enqueue("high", "first")
        queues = ["high", "low"]
        assert connector.pop_multiple(queues, timeout=0) == ("high", "first")
        assert connector.pop_multiple(queues, timeout=0) == ("low", "second")
        assert connector.pop_multiple(queues, timeout=0) is None

    def test_pop_all_returns_fifo_order_and_never_blocks(self, connector):
        for i in range(3):
            connector.enqueue("q", i)
        assert connector.pop_all("q") == [0, 1, 2]
        assert connector.pop_all("q") == []


class TestNamespace:
    def test_clean_namespace_resets_registry_and_ids(self, connector):
        q1, _ = register_two_workers(connector)
        first = connector.get_client_id()
        connector.enqueue(q1, 1)
        connector.enqueue(connector.get_responses_queue(first), 2)

        connector.clean_namespace()

        assert connector.methods_registry() == {}
        assert connector.workers_registry() == {}
        assert connector.pop_all(q1) == []
        assert connector.pop_all(connector.get_responses_queue(first)) == []
        assert connector.get_client_id() == first


def test_subclass_missing_a_primitive_cannot_be_instantiated():
    class Incomplete(Connector):
        def clean_namespace(self):
            pass

    with pytest.raises(TypeError):
        Incomplete()
