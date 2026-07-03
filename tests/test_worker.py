import time

import pytest
from conftest import add, boom

from distributed_processing.messages import is_ack, single_request
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker


def send_request(connector, serializer, queue_name, request):
    connector.enqueue(
        connector.get_requests_queue(queue_name), serializer.dumps(request)
    )


def pop_responses(connector, client_id, serializer):
    responses_queue = connector.get_responses_queue(client_id)
    return [serializer.loads(x) for x in connector.pop_all(responses_queue)]


class TestRoundTrip:
    def test_single_request_result(self, connector, worker):
        s = worker.serializer
        send_request(connector, s, "q", single_request("add", args=[2, 3], id="cli:1"))
        worker.run_once(timeout=0.1)

        responses = pop_responses(connector, "cli", s)
        assert len(responses) == 1
        r = responses[0]
        assert r["id"] == "cli:1"
        assert r["result"] == 5
        # internal keys must be cleaned before sending
        for internal in ("reply_to", "is_notification", "method"):
            assert internal not in r
        assert r["metadata"]["worker"] == worker.worker_id
        assert "execution_finish" in r["metadata"]["timing"]

    def test_unknown_method_returns_error(self, connector, worker):
        s = worker.serializer
        send_request(connector, s, "q", single_request("nope", args=[1], id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli", s)
        assert r["error"]["code"] == -32601

    def test_bad_params_returns_invalid_params_error(self, connector, worker):
        s = worker.serializer
        # add() takes two args -> TypeError -> -32602
        send_request(connector, s, "q", single_request("add", args=[1], id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli", s)
        assert r["error"]["code"] == -32602

    def test_remote_exception_returns_internal_error(self, connector, worker):
        s = worker.serializer
        send_request(connector, s, "q", single_request("boom", id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli", s)
        assert r["error"]["code"] == -32603
        assert "remote failure" in r["error"]["trace"]  # with_trace=True default

    def test_with_trace_false_omits_remote_trace(self, connector):
        s = JsonSerializer()
        w = Worker(s, connector, with_trace=False)
        w.add_requests_queue("q", {"boom": boom})
        send_request(connector, s, "q", single_request("boom", id="cli:1"))
        w.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli", s)
        assert r["error"]["code"] == -32603
        assert "trace" not in r["error"]

    def test_notification_produces_no_response(self, connector, worker):
        s = worker.serializer
        send_request(
            connector, s, "q", single_request("add", args=[1, 2], is_notification=True)
        )
        worker.run_once(timeout=0.1)

        assert pop_responses(connector, "cli", s) == []

    def test_ack_is_sent_before_result(self, connector, worker):
        s = worker.serializer
        send_request(
            connector, s, "q", single_request("add", args=[1, 2], id="cli:1", ack=True)
        )
        worker.run_once(timeout=0.1)

        responses = pop_responses(connector, "cli", s)
        assert len(responses) == 2
        assert is_ack(responses[0])
        assert responses[0]["ack"]["id"] == "cli:1"
        assert responses[1]["result"] == 3

    def test_batch_request(self, connector, worker):
        s = worker.serializer
        batch = [
            single_request("add", args=[1, 2], id="cli:1"),
            single_request("add", args=[3, 4], id="cli:2"),
            single_request("nope", id="cli:3"),
        ]
        send_request(connector, s, "q", batch)
        worker.run_once(timeout=0.1)

        (batch_response,) = pop_responses(connector, "cli", s)
        assert isinstance(batch_response, list)
        by_id = {r["id"]: r for r in batch_response}
        assert by_id["cli:1"]["result"] == 3
        assert by_id["cli:2"]["result"] == 7
        assert by_id["cli:3"]["error"]["code"] == -32601
        # internal keys must be cleaned in every response of the batch
        for r in batch_response:
            for internal in ("reply_to", "is_notification", "method"):
                assert internal not in r


class TestRun:
    def test_run_with_finite_timeout_returns(self, worker):
        t_0 = time.time()
        worker.run(timeout=0.5)
        elapsed = time.time() - t_0
        assert 0.4 <= elapsed <= 3.0

    def test_run_without_queues_raises(self, connector):
        w = Worker(JsonSerializer(), connector)
        with pytest.raises(ValueError):
            w.run_once(timeout=0.1)


class TestLifecycle:
    def test_context_manager_unregisters(self, connector):
        with Worker(JsonSerializer(), connector) as w:
            w.add_requests_queue("q", {"add": add})
            w.update_methods_registry()
            queue_ref = connector.get_requests_queue("q")
            assert w.worker_id in connector.workers_registry()[queue_ref]

        assert w.worker_id not in connector.workers_registry()[queue_ref]

    def test_close_is_idempotent(self, connector):
        unregister_calls = []
        connector.unregister_methods = lambda worker_id: unregister_calls.append(
            worker_id
        )
        w = Worker(JsonSerializer(), connector)
        w.close()
        w.close()
        assert unregister_calls == [w.worker_id]


class TestQueues:
    def test_shuffled_queues_respects_priority(self, connector):
        w = Worker(JsonSerializer(), connector)
        w.add_requests_queue("low_a", {"f": add}, priority=10)
        w.add_requests_queue("low_b", {"f": add}, priority=10)
        w.add_requests_queue("high", {"f": add}, priority=30)

        high_ref = connector.get_requests_queue("high")
        low_refs = {
            connector.get_requests_queue("low_a"),
            connector.get_requests_queue("low_b"),
        }
        for _ in range(10):
            queues = w.shuffled_queues()
            assert queues[0] == high_ref
            assert set(queues[1:]) == low_refs

    def test_add_function_creates_or_extends_queue(self, connector):
        w = Worker(JsonSerializer(), connector)
        w.add_function("q", "add", add)
        w.add_function("q", "add2", add)
        ref = connector.get_requests_queue("q")
        assert set(w.requests_queues[ref][0]) == {"add", "add2"}

    def test_register_only_public_queues(self, connector):
        w = Worker(JsonSerializer(), connector)
        w.add_requests_queue("public", {"f": add}, register=True)
        w.add_requests_queue("private", {"g": add}, register=False)
        w.update_methods_registry()

        assert connector.all_queues_for_method("f") == [
            connector.get_requests_queue("public")
        ]
        assert connector.all_queues_for_method("g") == []
