import pytest

from distributed_processing.messages import (
    ack,
    error_response,
    is_ack,
    is_batch_request,
    is_batch_response,
    is_error_response,
    is_single_request,
    is_single_response,
    result_response,
    single_request,
)


class TestSingleRequest:
    def test_minimal(self):
        sr = single_request("add", args=[1, 2], id="c:1")
        assert sr["method"] == "add"
        assert sr["args"] == [1, 2]
        assert sr["id"] == "c:1"
        assert sr["is_notification"] is False
        assert "request_sent" in sr["timing"]

    def test_id_required_if_not_notification(self):
        with pytest.raises(TypeError):
            single_request("add", args=[1, 2], id=None)

    def test_notification_has_no_id(self):
        sr = single_request("add", args=[1], is_notification=True)
        assert "id" not in sr
        assert sr["is_notification"] is True

    def test_args_and_kwargs_are_mutually_exclusive(self):
        with pytest.raises(TypeError):
            single_request("add", args=[1], kwargs={"b": 2}, id="c:1")

    def test_empty_args_and_kwargs_are_dropped(self):
        sr = single_request("add", args=[], kwargs={}, id="c:1")
        assert "args" not in sr
        assert "kwargs" not in sr

    def test_optional_keys(self):
        sr = single_request(
            "add", args=[1], id="c:1", reply_to="rq", ack=True, foo="bar"
        )
        assert sr["reply_to"] == "rq"
        assert sr["ack"] is True
        assert sr["options"] == {"foo": "bar"}


class TestResponses:
    def test_result_response(self):
        rr = result_response(42, id="c:1")
        assert rr == {"result": 42, "id": "c:1"}

    def test_error_response_known_code(self):
        er = error_response(-32601, id="c:1")
        assert er["id"] == "c:1"
        assert er["error"]["code"] == -32601
        assert "method" in er["error"]["message"].lower()

    def test_error_response_unknown_code_uses_default_message(self):
        er = error_response(-1, id="c:1", message="custom")
        assert er["error"]["message"] == "custom"

    def test_error_response_with_trace(self):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            er = error_response(-32603, id="c:1", with_trace=True)
        assert "boom" in er["error"]["trace"]


class TestPredicates:
    def test_request_predicates(self):
        sr = single_request("add", args=[1], id="c:1")
        assert is_single_request(sr)
        assert not is_batch_request(sr)
        assert is_batch_request([sr])
        assert not is_batch_request([])

    def test_response_predicates(self):
        rr = result_response(1, id="c:1")
        er = error_response(-32600, id="c:2")
        assert is_single_response(rr) and is_single_response(er)
        assert is_error_response(er) and not is_error_response(rr)
        assert is_batch_response([rr, er])
        assert not is_batch_response([])
        assert not is_single_response({"foo": 1})

    def test_ack(self):
        a = ack("c:1", "worker_1", "q")
        assert is_ack(a)
        assert a["ack"]["id"] == "c:1"
        assert not is_ack({"result": 1})
