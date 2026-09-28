import threading
import time

import pytest
from conftest import MemoryConnector, add

from distributed_processing.client import Client
from distributed_processing.exceptions import RemoteException
from distributed_processing.worker import Worker


class TestBasics:
    def test_generate_id_increments(self, client):
        id_1 = client.generate_id()
        id_2 = client.generate_id()
        prefix_1, n_1 = id_1.rsplit(":", 1)
        prefix_2, n_2 = id_2.rsplit(":", 1)
        assert prefix_1 == prefix_2 == client.client_id
        assert int(n_2) == int(n_1) + 1

    def test_generate_id_starts_at_creation_time(self, connector, monkeypatch):
        monkeypatch.setattr(time, "time_ns", lambda: 5_000_000)
        c = Client(connector, client_id="c")
        assert c.generate_id() == "c:5001"

    def test_reused_client_id_does_not_repeat_ids(self, connector, monkeypatch):
        # Two clients created one after the other with the same explicit
        # client_id: the counter is seeded with the creation time, so the
        # ids of the second are all greater than the ids of the first.
        monkeypatch.setattr(time, "time_ns", lambda: 1_000_000)
        first = Client(connector, client_id="c")
        ids_first = [first.generate_id() for _ in range(3)]
        monkeypatch.setattr(time, "time_ns", lambda: 2_000_000)
        second = Client(connector, client_id="c")
        ids_second = [second.generate_id() for _ in range(3)]
        assert set(ids_first).isdisjoint(ids_second)
        n = lambda id_: int(id_.rsplit(":", 1)[1])  # noqa: E731
        assert max(n(i) for i in ids_first) < min(n(i) for i in ids_second)

    def test_registry_uses_simple_queue_names(self, client):
        registry = client.registry()
        assert "add" in registry["methods"]
        assert registry["methods"]["add"] == ["q"]
        assert "q" in registry["workers"]

    def test_unknown_method_raises(self, client):
        with pytest.raises(ValueError):
            client.rpc_async("does_not_exist", [1])

    def test_notification_does_not_track_pending(self, connector, worker, client):
        id_, _ = client.send_single_request("add", args=[1, 2], is_notification=True)
        assert id_ is None
        assert None not in client.pending
        # a worker processes it without producing any response
        worker.run_once(timeout=0.1)
        assert client.wait_responses(timeout=0) == []

    def test_notifications_cache_is_flat(self, client):
        # A response without id is cached as a notification, one item each
        # (the client stamps them with finished_time).
        client._update_responses_cache([{"result": 1}])
        client._update_responses_cache([{"result": 2}])
        assert [n["result"] for n in client.notifications] == [1, 2]

    def test_shared_variables_go_through_the_connector(self, connector, client):
        client.set_variable("v", {"a": 1})
        assert connector.get_variable("v") == {"a": 1}
        assert client.get_variable("v") == {"a": 1}
        assert client.get_variable("missing", default=0) == 0
        assert client.variables() == ["v"]
        assert client.update_variable("v", lambda d: {**d, "b": 2}) == {"a": 1, "b": 2}
        assert client.delete_variable("v") is True
        assert client.variables() == []


class TestHeartbeats:
    def test_alive_workers_keeps_workers_without_heartbeat(
        self, connector, worker, client
    ):
        # The fixture worker never beats (run_once only): still alive.
        assert client.alive_workers() == {"q": [worker.worker_id]}

    def test_alive_workers_drops_stale_and_keeps_fresh(self, connector, worker, client):
        q2 = connector.get_requests_queue("q2")
        connector.register_methods({q2: {"add": add}}, "w_stale")
        connector.register_methods({q2: {"add": add}}, "w_fresh")
        connector.heartbeat("w_stale")
        time.sleep(0.2)
        connector.heartbeat("w_fresh")

        alive = client.alive_workers(max_age=0.1)
        assert alive == {"q": [worker.worker_id], "q2": ["w_fresh"]}

        # A queue whose workers are all dead is reported with an empty list.
        q3 = connector.get_requests_queue("q3")
        connector.register_methods({q3: {"add": add}}, "w_stale")
        assert client.alive_workers(max_age=0.1)["q3"] == []

    def test_alive_workers_keeps_a_slow_beating_worker(self, connector, client):
        # The worker published a 1 s interval: a client with a shorter
        # max_age still gives it three intervals.
        q2 = connector.get_requests_queue("q2")
        connector.register_methods({q2: {"add": add}}, "w_slow")
        connector.heartbeat("w_slow", interval=1.0)
        time.sleep(0.2)

        assert client.alive_workers(max_age=0.1)["q2"] == ["w_slow"]
        assert client.prune_dead_workers(max_age=0.1) == []

    def test_unset_max_age_uses_the_connector_default(self):
        class Quick(MemoryConnector):
            default_heartbeat_max_age = 0.1

        connector = Quick()
        q = connector.get_requests_queue("q")
        connector.register_methods({q: {"add": add}}, "w_stale")
        connector.heartbeat("w_stale")
        client = Client(connector, check_registry="cache")  # cache filled now
        time.sleep(0.2)

        # Without a refresh nothing is pruned: the cache still lists q.
        assert client.alive_workers(update=False) == {"q": []}
        assert client.prune_dead_workers() == ["w_stale"]
        assert client.alive_workers() == {}

    def test_update_registry_cache_prunes_with_the_connector_default(self):
        class Quick(MemoryConnector):
            default_heartbeat_max_age = 0.1

        connector = Quick()
        q2 = connector.get_requests_queue("q2")
        connector.register_methods({q2: {"add": add}}, "w_stale")
        connector.heartbeat("w_stale")
        client = Client(connector, check_registry="cache")  # fresh: kept
        assert client.registry()["workers"] == {"q2": ["w_stale"]}
        time.sleep(0.2)

        assert client.registry(update=True)["workers"] == {}
        assert connector.heartbeats() == {}

        # prune=False only reads.
        connector.register_methods({q2: {"add": add}}, "w_stale2")
        connector.heartbeat("w_stale2")
        time.sleep(0.2)
        client.update_registry_cache(prune=False)
        assert client.registry()["workers"] == {"q2": ["w_stale2"]}

    def test_prune_dead_workers_refreshes_the_cache(self, connector, worker, client):
        q2 = connector.get_requests_queue("q2")
        connector.register_methods({q2: {"mul": add}}, "w_stale")
        connector.heartbeat("w_stale")
        client.update_registry_cache()
        assert client.all_queues_for_method("mul") == ["q2"]
        time.sleep(0.2)

        assert client.prune_dead_workers(max_age=0.1) == ["w_stale"]
        assert "q2" not in client.registry()["workers"]
        assert client.all_workers_for_method("add") == [worker.worker_id]


class TestRpc:
    def test_rpc_sync_combines_positional_and_named_params(self, connector, client):
        def hola(nombre, calificativo="listo"):
            return f"Hola {nombre}, eres muy {calificativo}"

        w = Worker(connector)
        w.add_requests_queue("saludos", {"hola": hola}, register=False)

        f = client.rpc_async(
            "hola", ["Ana"], {"calificativo": "rápida"}, queue="saludos"
        )
        w.run_once(timeout=0.1)
        assert f.get(timeout=1) == "Hola Ana, eres muy rápida"

    def test_rpc_async_round_trip(self, connector, worker, client):
        f = client.rpc_async("add", [20, 22])
        worker.run_once(timeout=0.1)
        assert f.get(timeout=1) == 42

    def test_rpc_async_remote_error(self, connector, worker, client):
        f = client.rpc_async("boom")
        worker.run_once(timeout=0.1)
        with pytest.raises(RemoteException):
            f.get(timeout=1)
        assert f.safe_get(timeout=1, default="fallback") == "fallback"

    def test_rpc_multi_async(self, connector, worker, client):
        fs = client.rpc_multi_async([("add", [i, i], None) for i in range(5)])
        for _ in range(5):
            worker.run_once(timeout=0.1)
        assert [f.get(timeout=1) for f in fs] == [0, 2, 4, 6, 8]

    def test_rpc_async_fn_serializes_local_function(self, connector, client):
        # Worker with py_eval queue: executes dill-serialized local functions.
        w = Worker(connector)
        w.add_python_eval()
        w.update_methods_registry()
        client.update_registry_cache()

        f = client.rpc_async_fn(lambda a, b: a * b, args=[6, 7])
        w.run_once(timeout=0.1)
        assert f.get(timeout=1) == 42


class TestBatch:
    def _setup_two_workers(self, connector):
        """q1 only offers 'add'; q2 offers 'add' and 'mul'."""
        w1 = Worker(connector)
        w1.add_requests_queue("q1", {"add": add})
        w1.update_methods_registry()

        w2 = Worker(connector)
        w2.add_requests_queue("q2", {"add": add, "mul": lambda a, b: a * b})
        w2.update_methods_registry()

        client = Client(connector, check_registry="cache")
        return w1, w2, client

    def test_batch_goes_to_common_queue(self, connector):
        _, w2, client = self._setup_two_workers(connector)

        # Only q2 offers both methods: the batch must always land there.
        for _ in range(10):
            client.send_batch_request([("add", [1, 2], None), ("mul", [3, 4], None)])
            q2_ref = connector.get_requests_queue("q2")
            assert connector.pop_all(q2_ref) != []
            q1_ref = connector.get_requests_queue("q1")
            assert connector.pop_all(q1_ref) == []

    def test_batch_without_common_queue_raises(self, connector):
        _, _, client = self._setup_two_workers(connector)
        with pytest.raises(ValueError):
            client.send_batch_request([("add", [1], None), ("nope", [1], None)])

    def test_empty_batch_raises(self, connector):
        _, _, client = self._setup_two_workers(connector)
        with pytest.raises(ValueError):
            client.send_batch_request([])

    def test_explicit_queue_is_used_as_is(self, connector):
        w1, _, client = self._setup_two_workers(connector)

        # 'cache' mode, but the explicit queue is not checked against the
        # registry: the batch lands in q1 although q1 does not offer 'mul'.
        fs = client.rpc_batch_async(
            [("add", [1, 2], None), ("mul", [3, 4], None)], queue="q1"
        )
        w1.run_once(timeout=0.1)
        assert [f.safe_get(timeout=1) for f in fs] == [3, None]
        assert fs[1].error["code"] == -32601

    def test_cache_mode_does_not_fall_back_to_default_queue(self, connector):
        self._setup_two_workers(connector)
        client = Client(connector, check_registry="cache", default_queue="q1")

        with pytest.raises(ValueError):
            client.rpc_async("nope", [1])
        assert connector.pop_all(connector.get_requests_queue("q1")) == []

    def test_never_mode_uses_explicit_queue_as_is(self, connector):
        _, w2, _ = self._setup_two_workers(connector)
        client = Client(connector, check_registry="never", default_queue="default")

        # The registry is not consulted: q2 is used although 'nope' is
        # served nowhere. The worker answers -32601 for it.
        fs = client.rpc_batch_async(
            [("add", [1, 2], None), ("nope", [1], None)], queue="q2"
        )
        assert connector.pop_all(connector.get_requests_queue("default")) == []
        w2.run_once(timeout=0.1)
        assert [f.safe_get(timeout=1) for f in fs] == [3, None]

    def test_rpc_batch_sync_accepts_a_queue(self, connector):
        _, w2, client = self._setup_two_workers(connector)
        t = threading.Thread(target=w2.run, kwargs={"timeout": 2}, daemon=True)
        t.start()
        try:
            assert client.rpc_batch_sync(
                [("add", [1, 2], None), ("mul", [3, 4], None)], timeout=2, queue="q2"
            ) == [3, 12]
        finally:
            t.join()

    def test_rpc_batch_round_trip(self, connector):
        _, w2, client = self._setup_two_workers(connector)
        fs = client.rpc_batch_async([("add", [1, 2], None), ("mul", [3, 4], None)])
        w2.run_once(timeout=0.1)  # a batch is a single message
        assert [f.get(timeout=1) for f in fs] == [3, 12]


class TestCheckRegistry:
    def test_value_is_normalized(self, connector, worker):
        client = Client(connector, check_registry="Never")
        assert client.check_registry == "never"
        client.check_registry = " CACHE "
        assert client.check_registry == "cache"

    @pytest.mark.parametrize("bad", ["chache", True, None])
    def test_unknown_value_raises(self, connector, worker, bad):
        with pytest.raises(ValueError, match="cache"):
            Client(connector, check_registry=bad)
        client = Client(connector)
        with pytest.raises(ValueError, match="never"):
            client.check_registry = bad
        assert client.check_registry == "cache"  # unchanged

    def test_switching_to_cache_fills_the_cache(self, connector, worker):
        client = Client(connector, check_registry="never")
        assert client.registry() == {"methods": {}, "workers": {}}

        client.check_registry = "cache"

        assert "add" in client.registry()["methods"]
        f = client.rpc_async("add", [1, 2])  # no KeyError, queue found
        worker.run_once(timeout=0.1)
        assert f.get(timeout=1) == 3

    def test_unknown_method_has_no_workers(self, client):
        assert client.all_workers_for_method("nope") == []

    def test_stale_cache_is_refreshed_once_for_batches(self, connector):
        client = Client(connector, check_registry="cache")  # empty registry
        w = Worker(connector)
        w.add_requests_queue("late", {"add": add})
        w.update_methods_registry()

        fs = client.rpc_batch_async([("add", [1, 2], None)])
        w.run_once(timeout=0.1)
        assert fs[0].get(timeout=1) == 3


class TestRegistryQueries:
    def test_all_workers_for_method_multiple_queues(self, connector):
        w1 = Worker(connector)
        w1.add_requests_queue("q1", {"add": add})
        w1.update_methods_registry()

        w2 = Worker(connector)
        w2.add_requests_queue("q2", {"add": add})
        w2.update_methods_registry()

        client = Client(connector, check_registry="cache")
        workers = client.all_workers_for_method("add")
        assert workers == sorted([w1.worker_id, w2.worker_id])

    def test_all_queues_for_method(self, client):
        assert client.all_queues_for_method("add") == ["q"]
        assert client.all_queues_for_method("does_not_exist") == []
