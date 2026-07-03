import threading

import pytest

from distributed_processing.async_result import FAILED, OK, PENDING, gather
from distributed_processing.exceptions import RemoteException


class TestAsyncResult:
    def test_status_transitions_to_ok(self, connector, worker, client):
        f = client.rpc_async("add", [1, 2])
        assert f.pending()

        worker.run_once(timeout=0.1)
        assert f.status == OK
        assert f.ok() and f.done() and not f.failed()
        assert f.value == 3

    def test_status_transitions_to_failed(self, connector, worker, client):
        f = client.rpc_async("boom")
        worker.run_once(timeout=0.1)
        assert f.status == FAILED
        assert f.failed() and f.done()
        with pytest.raises(RemoteException):
            f.get(timeout=1)

    def test_get_timeout(self, client):
        f = client.rpc_async("add", [1, 2])  # no worker running
        with pytest.raises(TimeoutError):
            f.get(timeout=0.05)
        assert f.status == PENDING

    def test_retry_requires_request_info(self, client):
        f = client.rpc_async("add", [1, 2], retry=False)
        with pytest.raises(ValueError):
            f.retry()

    def test_retry_reuses_id(self, connector, worker, client):
        f = client.rpc_async("add", [1, 2], retry=True)
        # Simulate a lost message: drop the request from the queue.
        queue_ref = connector.get_requests_queue(f.queue)
        connector.queues[queue_ref].popleft()

        assert f.retry() is True
        assert f.retries == 1

        worker.run_once(timeout=0.1)
        assert f.get(timeout=1) == 3

    def test_retry_skipped_if_already_done(self, connector, worker, client):
        f = client.rpc_async("add", [1, 2], retry=True)
        worker.run_once(timeout=0.1)
        f.wait(timeout=1)
        assert f.retry() is False
        assert f.retries == 0


class TestGather:
    def test_gather_completed_results(self, connector, worker, client):
        fs = [client.rpc_async("add", [i, i]) for i in range(5)]
        for _ in range(5):
            worker.run_once(timeout=0.1)

        # timeout=None means unlimited waiting; everything is already there.
        assert gather(fs, timeout=None, step=0.1) is True
        assert [f.get(timeout=1) for f in fs] == [0, 2, 4, 6, 8]

    def test_gather_timeout_returns_false(self, connector, worker, client):
        fs = [client.rpc_async("add", [1, 1])]  # no worker running
        assert gather(fs, timeout=0.3, step=0.1, max_dt=10) is False

    def test_gather_empty_list(self, client):
        assert gather([], timeout=1, step=0.1) is True

    def test_gather_retries_lost_requests(self, connector, worker, client):
        f1 = client.rpc_async("add", [1, 1], retry=True)
        f2 = client.rpc_async("add", [2, 2], retry=True)

        # Simulate the loss of f1's request: drop it from the queue before
        # the worker starts. FIFO queues: f2 completing while f1 is still
        # pending marks f1 as lost, and gather must retry it.
        queue_ref = connector.get_requests_queue(f1.queue)
        connector.queues[queue_ref].popleft()

        worker_thread = threading.Thread(
            target=worker.run, kwargs={"timeout": 5}, daemon=True
        )
        worker_thread.start()

        assert gather([f1, f2], timeout=10, step=0.2, max_dt=0.05) is True
        assert f1.retries == 1
        assert f1.get(timeout=1) == 2
        assert f2.get(timeout=1) == 4
