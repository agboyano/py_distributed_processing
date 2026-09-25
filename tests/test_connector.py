"""Behavioural contract of `Connector`, checked against every implementation.

Each test is one rule of the contract documented in
`distributed_processing/connector.py`. The filesystem connector is an
integration case (real directory); the Redis one runs on `FakeRedis`.
"""

import threading
import time

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


class TestVariables:
    def test_set_get_round_trip_of_a_python_object(self, connector):
        connector.set_variable("params", {"a": [1, 2], "b": "x"})
        assert connector.get_variable("params") == {"a": [1, 2], "b": "x"}

    def test_get_returns_a_copy(self, connector):
        connector.set_variable("v", {"n": 1})
        connector.get_variable("v")["n"] = 2
        assert connector.get_variable("v") == {"n": 1}

    def test_missing_variable_returns_default(self, connector):
        assert connector.get_variable("nope") is None
        assert connector.get_variable("nope", default=0) == 0

    def test_last_write_wins(self, connector):
        connector.set_variable("v", 1)
        connector.set_variable("v", 2)
        assert connector.get_variable("v") == 2

    def test_delete_reports_whether_it_existed(self, connector):
        connector.set_variable("v", 1)
        assert connector.delete_variable("v") is True
        assert connector.delete_variable("v") is False
        assert connector.get_variable("v") is None

    def test_variables_lists_sorted_names_without_prefix(self, connector):
        connector.set_variable("b", 1)
        connector.set_variable("a", 2)
        assert connector.variables() == ["a", "b"]

    def test_update_variable_uses_default_and_returns_the_new_value(self, connector):
        assert connector.update_variable("n", lambda x: x + 1, default=0) == 1
        assert connector.update_variable("n", lambda x: x + 1, default=0) == 2
        assert connector.get_variable("n") == 2

    def test_update_variable_of_a_missing_variable_without_default_raises(
        self, connector
    ):
        with pytest.raises(KeyError, match="missing"):
            connector.update_variable("missing", lambda x: x + 1)
        assert connector.variables() == []
        # An explicit None is a valid default.
        assert connector.update_variable("v", lambda x: [x], default=None) == [None]
        # Once set, no default is needed.
        assert connector.update_variable("v", lambda x: x + [1]) == [None, 1]

    def test_update_variable_leaves_the_value_unchanged_if_fn_raises(self, connector):
        connector.set_variable("n", 5)

        def boom(x):
            raise RuntimeError("no")

        with pytest.raises(RuntimeError):
            connector.update_variable("n", boom)
        assert connector.get_variable("n") == 5
        # the lock was released: the next update goes through
        assert connector.update_variable("n", lambda x: x + 1) == 6

    def test_update_variable_does_not_lose_concurrent_updates(self, connector):
        threads, per_thread = 4, 5

        def count():
            for _ in range(per_thread):
                connector.update_variable("n", lambda x: x + 1, default=0)

        workers = [threading.Thread(target=count) for _ in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()

        assert connector.get_variable("n") == threads * per_thread
        assert connector.variables() == ["n"]  # no lock shows up as a variable

    def test_variables_and_registry_do_not_mix(self, connector):
        q1, _ = register_two_workers(connector)
        connector.set_variable("add", "not a method")
        assert connector.variables() == ["add"]
        assert sorted(connector.all_queues_for_method("add")) == sorted(
            connector.methods_registry()["add"]
        )
        connector.unregister_methods("w1")
        connector.unregister_methods("w2")
        assert connector.get_variable("add") == "not a method"


class TestHeartbeats:
    def test_heartbeat_is_recorded_and_deleted(self, connector):
        before = time.time()
        connector.heartbeat("w1")
        beats = connector.heartbeats()
        assert set(beats) == {"w1"}
        assert before <= beats["w1"] <= time.time()

        assert connector.delete_heartbeat("w1") is True
        assert connector.delete_heartbeat("w1") is False
        assert connector.heartbeats() == {}

    def test_alive_workers_expire_with_max_age(self, connector):
        connector.heartbeat("old")
        time.sleep(0.2)
        connector.heartbeat("fresh")

        assert connector.alive_workers(max_age=60) == {"old", "fresh"}
        assert connector.alive_workers(max_age=0.1) == {"fresh"}

    def test_heartbeat_publishes_the_interval(self, connector):
        connector.heartbeat("w1", interval=60)
        connector.heartbeat("w2")
        assert connector.heartbeat_intervals() == {"w1": 60}
        assert set(connector.heartbeats()) == {"w1", "w2"}

        assert connector.delete_heartbeat("w1") is True
        assert connector.heartbeat_intervals() == {}
        assert set(connector.heartbeats()) == {"w2"}

    def test_dead_workers_use_the_worker_interval(self, connector):
        q1, q2 = register_two_workers(connector)  # w1 on q1, w2 on q1 and q2
        # w1 beats slowly and says so: three intervals are its tolerance.
        connector.heartbeat("w1", interval=1.0)
        # w2 beats without an interval: max_age is its tolerance.
        beat = time.time()
        connector.heartbeat("w2")
        time.sleep(0.2)

        dead = connector.dead_workers(max_age=0.1)
        assert set(dead) == {"w2"}
        assert abs(dead["w2"] - (beat + 0.1)) < 0.05
        assert connector.alive_workers(max_age=0.1) == {"w1"}
        assert connector.dead_workers(max_age=60) == {}

        assert connector.prune_dead_workers(max_age=0.1) == ["w2"]
        assert connector.workers_registry() == {q1: ["w1"]}


class TestHeartbeatDefaults:
    def test_defaults_are_class_attributes_of_the_transport(self):
        assert MemoryConnector.default_heartbeat_interval == 10
        assert MemoryConnector.default_heartbeat_max_age == 30
        pytest.importorskip("fs_structs")
        from distributed_processing.filesystem_connector import FileSystemConnector

        assert FileSystemConnector.default_heartbeat_interval == 30
        assert FileSystemConnector.default_heartbeat_max_age == 61

    def test_unset_max_age_uses_the_connector_default(self):
        class Quick(MemoryConnector):
            default_heartbeat_max_age = 0.1

        c = Quick()
        register_two_workers(c)  # w1 and w2
        c.heartbeat("w1")
        time.sleep(0.2)
        c.heartbeat("w2")

        assert set(c.dead_workers()) == {"w1"}
        assert c.alive_workers() == {"w2"}
        assert c.prune_dead_workers() == ["w1"]

    def test_heartbeats_are_not_variables(self, connector):
        connector.heartbeat("w1")
        connector.set_variable("w1", "a variable, not a heartbeat")
        assert connector.variables() == ["w1"]
        assert connector.get_variable("w1") == "a variable, not a heartbeat"
        assert set(connector.heartbeats()) == {"w1"}
        connector.delete_variable("w1")
        assert set(connector.heartbeats()) == {"w1"}

    def test_prune_dead_workers_only_removes_stale_heartbeats(self, connector):
        q1, q2 = register_two_workers(connector)  # w1 and w2, no heartbeats yet
        q3 = connector.get_requests_queue("q3")
        connector.register_methods({q3: {"mul": fn}}, "w3")

        connector.heartbeat("w3")  # will go stale
        time.sleep(0.2)
        connector.heartbeat("w2")  # fresh; w1 never beats

        assert connector.prune_dead_workers(max_age=0.1) == ["w3"]

        workers = connector.workers_registry()
        assert q3 not in workers  # w3 was its only worker
        assert sorted(workers[q1]) == ["w1", "w2"]  # w1 kept: no heartbeat key
        assert workers[q2] == ["w2"]
        assert set(connector.heartbeats()) == {"w2"}
        # Idempotent: nothing stale is left (a large max_age keeps w2 fresh
        # even on a slow filesystem).
        assert connector.prune_dead_workers(max_age=60) == []


class TestNamespace:
    def test_clean_namespace_resets_registry_and_ids(self, connector):
        q1, _ = register_two_workers(connector)
        first = connector.get_client_id()
        connector.enqueue(q1, 1)
        connector.enqueue(connector.get_responses_queue(first), 2)
        connector.set_variable("v", 1)
        connector.heartbeat("w1")

        connector.clean_namespace()

        assert connector.methods_registry() == {}
        assert connector.workers_registry() == {}
        assert connector.pop_all(q1) == []
        assert connector.pop_all(connector.get_responses_queue(first)) == []
        assert connector.get_client_id() == first
        assert connector.get_variable("v") is None
        assert connector.variables() == []
        assert connector.heartbeats() == {}


def test_subclass_missing_a_primitive_cannot_be_instantiated():
    class Incomplete(Connector):
        def clean_namespace(self):
            pass

    with pytest.raises(TypeError):
        Incomplete()
