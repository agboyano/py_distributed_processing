import pytest
from conftest import add

from distributed_processing.client import Client
from distributed_processing.exceptions import RemoteException
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker


class TestBasics:
    def test_generate_id_increments(self, client):
        id_1 = client.generate_id()
        id_2 = client.generate_id()
        assert id_1 == f"{client.client_id}:1"
        assert id_2 == f"{client.client_id}:2"

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
        s = client.serializer
        client._update_responses_cache([s.dumps({"result": 1})])
        client._update_responses_cache([s.dumps({"result": 2})])
        assert [n["result"] for n in client.notifications] == [1, 2]

    def test_parse_errors_cache_is_flat(self, client):
        client._update_responses_cache([b"not json at all"])
        assert client.responses_parse_errors == [b"not json at all"]


class TestRpc:
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
        w = Worker(JsonSerializer(), connector)
        w.add_python_eval()
        w.update_methods_registry()
        client.update_registry_cache()

        f = client.rpc_async_fn(lambda a, b: a * b, args=[6, 7])
        w.run_once(timeout=0.1)
        assert f.get(timeout=1) == 42


class TestBatch:
    def _setup_two_workers(self, connector):
        """q1 only offers 'add'; q2 offers 'add' and 'mul'."""
        w1 = Worker(JsonSerializer(), connector)
        w1.add_requests_queue("q1", {"add": add})
        w1.update_methods_registry()

        w2 = Worker(JsonSerializer(), connector)
        w2.add_requests_queue("q2", {"add": add, "mul": lambda a, b: a * b})
        w2.update_methods_registry()

        client = Client(JsonSerializer(), connector, check_registry="cache")
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

    def test_explicit_queue_must_be_common(self, connector):
        _, _, client = self._setup_two_workers(connector)
        with pytest.raises(ValueError):
            client.send_batch_request(
                [("add", [1, 2], None), ("mul", [3, 4], None)], queue="q1"
            )

    def test_rpc_batch_round_trip(self, connector):
        _, w2, client = self._setup_two_workers(connector)
        fs = client.rpc_batch_async([("add", [1, 2], None), ("mul", [3, 4], None)])
        w2.run_once(timeout=0.1)  # a batch is a single message
        assert [f.get(timeout=1) for f in fs] == [3, 12]


class TestRegistryQueries:
    def test_all_workers_for_method_multiple_queues(self, connector):
        w1 = Worker(JsonSerializer(), connector)
        w1.add_requests_queue("q1", {"add": add})
        w1.update_methods_registry()

        w2 = Worker(JsonSerializer(), connector)
        w2.add_requests_queue("q2", {"add": add})
        w2.update_methods_registry()

        client = Client(JsonSerializer(), connector, check_registry="cache")
        workers = client.all_workers_for_method("add")
        assert workers == sorted([w1.worker_id, w2.worker_id])

    def test_all_queues_for_method(self, client):
        assert client.all_queues_for_method("add") == ["q"]
        assert client.all_queues_for_method("does_not_exist") == []
