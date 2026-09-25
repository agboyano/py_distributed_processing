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

    # --- retry_lost -------------------------------------------------------
    #
    # A worker pops a request before running it, so "taken by a worker that
    # died" is simulated by dropping the head of the queue by hand. All the
    # gather calls use max_age=0.1: a worker that beat once and then slept
    # 0.2 s looks dead while the registry still lists it.

    LOST = dict(step=0.1, retry_lost=True, max_age=0.1)

    def _worker_on_q2(self, connector, methods=None):
        w = Worker(connector, heartbeat_interval=None)
        w.add_requests_queue("q2", methods or {"add": add})
        w.update_methods_registry()
        return w

    def _dies(self, connector, w):
        "One last beat, then long enough for it to be older than max_age."
        connector.heartbeat(w.worker_id)
        time.sleep(0.2)

    def _drop_head(self, connector, queue):
        connector.queues[connector.get_requests_queue(queue)].popleft()

    def _queue_len(self, connector, queue):
        return len(connector.queues.get(connector.get_requests_queue(queue), []))

    def test_retry_lost_resends_a_request_taken_by_a_dead_worker(
        self, connector, worker
    ):
        w2 = self._worker_on_q2(connector)
        client = Client(connector)
        f1 = client.rpc_async("add", [5, 5], queue="q2", retry=True)
        f2 = client.rpc_async("add", [1, 1], queue="q2", retry=True)
        self._drop_head(connector, "q2")  # w2 took f1 and never answered
        w2.run_once(timeout=0.1)  # ...but it answered f2: f1 was popped
        self._dies(connector, w2)
        run_in_thread(worker, timeout=3)  # the fixture worker serves q

        assert gather([f1, f2], timeout=5, **self.LOST) == []
        assert f1.retries == 1 and f1.queue == "q" and f1.lost is False
        assert f1.get(timeout=1) == 10 and f2.get(timeout=1) == 2

    def test_retry_lost_prefers_the_same_queue_when_a_new_worker_serves_it(
        self, connector, worker
    ):
        w2 = self._worker_on_q2(connector)
        client = Client(connector)
        f1 = client.rpc_async("add", [5, 5], queue="q2", retry=True)
        f2 = client.rpc_async("add", [1, 1], queue="q2", retry=True)
        self._drop_head(connector, "q2")
        w2.run_once(timeout=0.1)
        self._dies(connector, w2)
        run_in_thread(self._worker_on_q2(connector), timeout=3)  # replacement
        run_in_thread(worker, timeout=3)  # q also serves "add"

        assert gather([f1, f2], timeout=5, **self.LOST) == []
        assert f1.retries == 1 and f1.queue == "q2"
        assert f1.get(timeout=1) == 10

    def test_retry_lost_uses_the_answers_of_other_clients(self, connector, worker):
        w2 = self._worker_on_q2(connector)
        c1, c2 = Client(connector), Client(connector)
        f1 = c1.rpc_async("add", [5, 5], queue="q2", retry=True)
        f2 = c2.rpc_async("add", [1, 1], queue="q2", retry=True)
        self._drop_head(connector, "q2")  # w2 took f1 (client 1)
        w2.run_once(timeout=0.1)  # and answered f2 (client 2)
        self._dies(connector, w2)
        run_in_thread(worker, timeout=3)

        assert gather([f1, f2], timeout=5, **self.LOST) == []
        assert f1.retries == 1 and f1.get(timeout=1) == 10

    def test_retry_lost_cannot_check_the_newest_request(self, connector):
        # Nothing was sent after f, so nothing can prove that w2 took it,
        # even with a new worker on q2. Documented gap.
        w2 = self._worker_on_q2(connector)
        client = Client(connector)
        f = client.rpc_async("add", [2, 3], queue="q2", retry=True)
        self._drop_head(connector, "q2")
        self._dies(connector, w2)
        run_in_thread(self._worker_on_q2(connector), timeout=1)

        assert gather([f], timeout=0.3, **self.LOST) == [f]
        assert f.retries == 0 and f.lost is False

    def test_retry_lost_leaves_a_request_still_in_the_queue(self, connector):
        w2 = self._worker_on_q2(connector)
        client = Client(connector)
        f = client.rpc_async("add", [2, 3], queue="q2", retry=True)
        self._dies(connector, w2)  # dead, but f is still in the queue

        assert gather([f], timeout=0.3, **self.LOST) == [f]
        assert f.retries == 0 and f.lost is False
        assert self._queue_len(connector, "q2") == 1

    def test_retry_lost_skips_when_every_worker_is_alive(self, connector, worker):
        # The fixture worker has no heartbeat, so it counts as alive.
        client = Client(connector)
        f1 = client.rpc_async("add", [5, 5], queue="q", retry=True)
        f2 = client.rpc_async("add", [1, 1], queue="q", retry=True)
        self._drop_head(connector, "q")
        worker.run_once(timeout=0.1)  # answers f2: f1 popped, worker alive

        assert gather([f1, f2], timeout=0.3, **self.LOST) == [f1]
        assert f1.retries == 0 and f1.lost is False

    def test_retry_lost_ignores_a_worker_dead_before_the_request_was_sent(
        self, connector
    ):
        self._dies(connector, self._worker_on_q2(connector))  # stale entry
        w3 = self._worker_on_q2(connector)  # alive: no heartbeat
        client = Client(connector)
        f1 = client.rpc_async("add", [5, 5], queue="q2", retry=True)
        f2 = client.rpc_async("add", [1, 1], queue="q2", retry=True)
        self._drop_head(connector, "q2")
        w3.run_once(timeout=0.1)  # answers f2; f1 may be running in w3

        assert gather([f1, f2], timeout=0.3, **self.LOST) == [f1]
        assert f1.retries == 0 and f1.lost is False

    def test_retry_lost_warns_and_resends_when_a_worker_appears(
        self, connector, caplog
    ):
        mul = {"mul": lambda a, b: a * b}
        w2 = self._worker_on_q2(connector, mul)  # only q2 serves "mul"
        client = Client(connector)
        f1 = client.rpc_async("mul", [2, 3], retry=True)
        f2 = client.rpc_async("mul", [1, 1], retry=True)
        self._drop_head(connector, "q2")
        w2.run_once(timeout=0.1)
        self._dies(connector, w2)

        with caplog.at_level("WARNING", logger="distributed_processing.async_result"):
            assert gather([f1, f2], timeout=0.3, **self.LOST) == [f1]
        assert f1.lost is True and f1.retries == 0
        assert self._queue_len(connector, "q2") == 0  # nothing resent
        assert caplog.text.count("will be resent when one appears") == 1

        run_in_thread(self._worker_on_q2(connector, mul), timeout=3)
        assert gather([f1], timeout=5, **self.LOST) == []
        assert f1.retries == 1 and f1.queue == "q2" and f1.get(timeout=1) == 6

    def test_retry_lost_flags_but_does_not_resend_without_retry_info(
        self, connector, worker, caplog
    ):
        w2 = self._worker_on_q2(connector)
        client = Client(connector)
        no_info = client.rpc_async("add", [1, 1], queue="q2", retry=False)
        f2 = client.rpc_async("add", [2, 2], queue="q2", retry=True)
        self._drop_head(connector, "q2")
        w2.run_once(timeout=0.1)
        self._dies(connector, w2)
        run_in_thread(worker, timeout=3)  # q is alive and serves "add"

        with caplog.at_level("WARNING", logger="distributed_processing.async_result"):
            assert gather([no_info, f2], timeout=0.5, **self.LOST) == [no_info]
        assert no_info.lost is True and no_info.retries == 0
        assert "created without retry=True" in caplog.text
