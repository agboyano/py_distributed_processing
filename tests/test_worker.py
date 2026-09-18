import time

import pytest
from conftest import add, boom

from distributed_processing.messages import is_ack, single_request
from distributed_processing.worker import Worker


def send_request(connector, queue_name, request):
    connector.enqueue(connector.get_requests_queue(queue_name), request)


def pop_responses(connector, client_id):
    return connector.pop_all(connector.get_responses_queue(client_id))


class TestRoundTrip:
    def test_single_request_result(self, connector, worker):
        send_request(connector, "q", single_request("add", args=[2, 3], id="cli:1"))
        worker.run_once(timeout=0.1)

        responses = pop_responses(connector, "cli")
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
        send_request(connector, "q", single_request("nope", args=[1], id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32601

    def test_bad_params_returns_invalid_params_error(self, connector, worker):
        # add() takes two args -> TypeError -> -32602
        send_request(connector, "q", single_request("add", args=[1], id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32602

    def test_type_error_inside_function_is_internal_error(self, connector):
        def bad_concat(x):
            return "a" + x  # TypeError raised *inside* the function

        w = Worker(connector)
        w.add_requests_queue("q", {"bad_concat": bad_concat})
        send_request(connector, "q", single_request("bad_concat", args=[1], id="cli:1"))
        w.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32603
        assert "TypeError" in r["error"]["trace"]

    def test_bad_kwargs_returns_invalid_params_error(self, connector, worker):
        send_request(
            connector, "q", single_request("add", kwargs={"a": 1, "z": 2}, id="cli:1")
        )
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32602

    def test_function_without_signature_is_still_called(self, connector):
        # Some builtins have no inspectable signature: the params check is
        # skipped and the call goes through.
        class NoSignature:
            @property
            def __signature__(self):
                raise ValueError("no signature found")

            def __call__(self, x):
                return x * 2

        w = Worker(connector)
        w.add_requests_queue("q", {"twice": NoSignature()})
        send_request(connector, "q", single_request("twice", args=[21], id="cli:1"))
        w.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["result"] == 42

    def test_remote_exception_returns_internal_error(self, connector, worker):
        send_request(connector, "q", single_request("boom", id="cli:1"))
        worker.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32603
        assert "remote failure" in r["error"]["trace"]  # with_trace=True default

    def test_with_trace_false_omits_remote_trace(self, connector):
        w = Worker(connector, with_trace=False)
        w.add_requests_queue("q", {"boom": boom})
        send_request(connector, "q", single_request("boom", id="cli:1"))
        w.run_once(timeout=0.1)

        (r,) = pop_responses(connector, "cli")
        assert r["error"]["code"] == -32603
        assert "trace" not in r["error"]

    def test_notification_produces_no_response(self, connector, worker):
        send_request(
            connector, "q", single_request("add", args=[1, 2], is_notification=True)
        )
        worker.run_once(timeout=0.1)

        assert pop_responses(connector, "cli") == []

    def test_ack_is_sent_before_result(self, connector, worker):
        send_request(
            connector, "q", single_request("add", args=[1, 2], id="cli:1", ack=True)
        )
        worker.run_once(timeout=0.1)

        responses = pop_responses(connector, "cli")
        assert len(responses) == 2
        assert is_ack(responses[0])
        assert responses[0]["ack"]["id"] == "cli:1"
        assert responses[1]["result"] == 3

    def test_batch_request(self, connector, worker):
        batch = [
            single_request("add", args=[1, 2], id="cli:1"),
            single_request("add", args=[3, 4], id="cli:2"),
            single_request("nope", id="cli:3"),
        ]
        send_request(connector, "q", batch)
        worker.run_once(timeout=0.1)

        (batch_response,) = pop_responses(connector, "cli")
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
        w = Worker(connector)
        with pytest.raises(ValueError):
            w.run_once(timeout=0.1)


class FlakyConnector:
    """Wraps a connector so that `pop_multiple` raises the first `n` times."""

    def __init__(self, connector, failures, exc=None):
        self._connector = connector
        self.failures = failures
        self.exc = PermissionError("drive not ready") if exc is None else exc
        self.calls = 0

    def pop_multiple(self, queues, timeout=-1):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc
        return self._connector.pop_multiple(queues, timeout)

    def __getattr__(self, name):
        return getattr(self._connector, name)


class TestRunForever:
    def _worker_that_stops_itself(self, connector):
        w = Worker(connector)

        def halt():
            w.stop()
            return "bye"

        w.add_requests_queue("q", {"add": add, "halt": halt})
        return w

    def test_stop_from_registered_function(self, connector):
        w = self._worker_that_stops_itself(connector)
        send_request(connector, "q", single_request("add", args=[1, 2], id="cli:1"))
        send_request(connector, "q", single_request("halt", id="cli:2"))

        w.run_forever(backoff=(0.01, 0.02))

        by_id = {r["id"]: r for r in pop_responses(connector, "cli")}
        assert by_id["cli:1"]["result"] == 3
        assert by_id["cli:2"]["result"] == "bye"

    def test_survives_connector_errors_with_backoff(self, connector, caplog):
        flaky = FlakyConnector(connector, failures=2)
        w = self._worker_that_stops_itself(flaky)
        send_request(connector, "q", single_request("halt", id="cli:1"))

        t_0 = time.time()
        with caplog.at_level("ERROR", logger="distributed_processing.worker"):
            w.run_forever(backoff=(0.05, 1.0))
        elapsed = time.time() - t_0

        # two failures: waits 0.05 + 0.10 before the third, successful, pop
        assert flaky.calls == 3
        assert elapsed >= 0.15
        assert sum("error #" in m for m in caplog.messages) == 2
        (r,) = pop_responses(connector, "cli")
        assert r["result"] == "bye"

    def test_max_consecutive_errors_reraises(self, connector):
        flaky = FlakyConnector(connector, failures=10)
        w = self._worker_that_stops_itself(flaky)

        with pytest.raises(PermissionError):
            w.run_forever(backoff=(0.001, 0.002), max_consecutive_errors=3)
        assert flaky.calls == 3

    def test_without_queues_raises(self, connector):
        w = Worker(connector)
        with pytest.raises(ValueError):
            w.run_forever()

    def test_keyboard_interrupt_is_not_swallowed(self, connector):
        flaky = FlakyConnector(connector, failures=1, exc=KeyboardInterrupt())
        w = self._worker_that_stops_itself(flaky)
        with pytest.raises(KeyboardInterrupt):
            w.run_forever(backoff=(0.001, 0.002))


class TestLifecycle:
    def test_context_manager_unregisters(self, connector):
        with Worker(connector) as w:
            w.add_requests_queue("q", {"add": add})
            w.update_methods_registry()
            queue_ref = connector.get_requests_queue("q")
            assert w.worker_id in connector.workers_registry()[queue_ref]

        # The queue had a single worker, so unregistering drops it entirely.
        assert queue_ref not in connector.workers_registry()

    def test_close_is_idempotent(self, connector):
        unregister_calls = []
        connector.unregister_methods = lambda worker_id: unregister_calls.append(
            worker_id
        )
        w = Worker(connector)
        w.close()
        w.close()
        assert unregister_calls == [w.worker_id]


class TestQueues:
    def test_shuffled_queues_respects_priority(self, connector):
        w = Worker(connector)
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
        w = Worker(connector)
        w.add_function("q", "add", add)
        w.add_function("q", "add2", add)
        ref = connector.get_requests_queue("q")
        assert set(w.requests_queues[ref][0]) == {"add", "add2"}

    def test_register_only_public_queues(self, connector):
        w = Worker(connector)
        w.add_requests_queue("public", {"f": add}, register=True)
        w.add_requests_queue("private", {"g": add}, register=False)
        w.update_methods_registry()

        assert connector.all_queues_for_method("f") == [
            connector.get_requests_queue("public")
        ]
        assert connector.all_queues_for_method("g") == []
