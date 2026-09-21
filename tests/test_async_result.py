import threading
import time

import pytest
from conftest import add

from distributed_processing.async_result import FAILED, OK, PENDING, gather
from distributed_processing.client import Client
from distributed_processing.exceptions import RemoteException
from distributed_processing.worker import Worker


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


def run_in_thread(worker, timeout):
    t = threading.Thread(target=worker.run, kwargs={"timeout": timeout}, daemon=True)
    t.start()
    return t


class TestGather:
    def test_all_received_returns_empty_list(self, connector, worker, client):
        fs = [client.rpc_async("add", [i, i]) for i in range(5)]
        for _ in range(5):
            worker.run_once(timeout=0.1)

        # timeout=None means unlimited waiting; everything is already there.
        assert gather(fs, timeout=None, step=0.1) == []
        assert [f.get(timeout=1) for f in fs] == [0, 2, 4, 6, 8]

    def test_empty_list(self, client):
        assert gather([], timeout=1, step=0.1) == []

    def test_timeout_returns_pending_with_a_common_deadline(self, connector, worker):
        # No worker running. Two clients share the deadline: 0.3 s in total,
        # not 0.3 s per client.
        c1, c2 = Client(connector), Client(connector)
        f1, f2 = c1.rpc_async("add", [1, 1]), c2.rpc_async("add", [2, 2])

        t_0 = time.time()
        pending = gather([f1, f2], timeout=0.3, step=0.1)
        elapsed = time.time() - t_0

        assert pending == [f1, f2]
        assert 0.25 <= elapsed < 0.6
        assert f1.pending() and f2.pending()

    def test_two_clients_one_worker(self, connector, worker):
        c1, c2 = Client(connector), Client(connector)
        fs = [
            c1.rpc_async("add", [1, 1]),
            c2.rpc_async("add", [2, 2]),
            c1.rpc_async("add", [3, 3]),
        ]
        run_in_thread(worker, timeout=3)

        assert gather(fs, timeout=5, step=0.1) == []
        assert [f.get(timeout=1) for f in fs] == [2, 4, 6]

    def test_already_consumed_results_do_not_raise(self, connector, worker, client):
        done = client.rpc_async("add", [1, 1])
        worker.run_once(timeout=0.1)
        assert done.get(timeout=1) == 2  # consumed and cleaned from the client cache

        pending = client.rpc_async("add", [2, 2])  # no worker running now
        assert gather([done, pending], timeout=0.2, step=0.1) == [pending]

    def _dead_worker_on_q2(self, connector):
        # w2 serves "add" on q2, beats once and "dies": its heartbeat goes
        # stale while the registry still lists it.
        w2 = Worker(connector, heartbeat_interval=None)
        w2.add_requests_queue("q2", {"add": add})
        w2.update_methods_registry()
        connector.heartbeat(w2.worker_id)
        return w2

    def test_retry_dead_resends_to_a_queue_with_alive_workers(self, connector, worker):
        self._dead_worker_on_q2(connector)
        client = Client(connector)
        f = client.rpc_async("add", [5, 5], queue="q2", retry=True)
        time.sleep(0.2)  # older than max_age below
        run_in_thread(worker, timeout=3)  # the fixture worker serves q

        assert gather([f], timeout=5, step=0.1, retry_dead=True, max_age=0.1) == []
        assert f.retries == 1
        assert f.queue == "q"
        assert f.get(timeout=1) == 10

    def test_retry_dead_skips_no_retry_info_and_alive_queues(self, connector, worker):
        self._dead_worker_on_q2(connector)
        client = Client(connector)
        no_info = client.rpc_async("add", [1, 1], queue="q2", retry=False)
        alive = client.rpc_async("add", [2, 2], queue="q", retry=True)
        time.sleep(0.2)
        # No worker running: the fixture worker has no heartbeat, so q counts
        # as alive and nothing is resent there; q2 is dead but no_info cannot
        # be retried.
        pending = gather(
            [no_info, alive], timeout=0.3, step=0.1, retry_dead=True, max_age=0.1
        )

        assert pending == [no_info, alive]
        assert no_info.retries == 0 and no_info.queue == "q2"
        assert alive.retries == 0 and alive.queue == "q"

    def test_retry_dead_resends_once_to_the_same_queue_if_no_alternative(
        self, connector
    ):
        # Only q2 serves "mul", and its worker is dead: resend once to q2.
        w2 = Worker(connector, heartbeat_interval=None)
        w2.add_requests_queue("q2", {"mul": lambda a, b: a * b})
        w2.update_methods_registry()
        connector.heartbeat(w2.worker_id)
        client = Client(connector)
        f = client.rpc_async("mul", [2, 3], retry=True)
        time.sleep(0.2)

        assert gather([f], timeout=0.5, step=0.1, retry_dead=True, max_age=0.1) == [f]
        assert f.retries == 1 and f.queue == "q2"
        assert len(connector.queues[connector.get_requests_queue("q2")]) == 2
