from __future__ import annotations

import logging
import random
from datetime import datetime
from time import time
from typing import Any

from .exceptions import RemoteException

logger = logging.getLogger(__name__)


def timestamp() -> str:
    return datetime.now().isoformat()


PENDING = "PENDING"
OK = "OK"
FAILED = "FAILED"
CLEANED = "CLEANED"


class AsyncResult:
    """Handle for the pending response of an asynchronous request.

    Returned by the `rpc_*_async` methods of `Client`. The result is
    obtained with `get`/`safe_get` and the state ('PENDING', 'OK' or
    'FAILED') is available in the `status` property.

    Args:
        rpc_client (Client): Client that sent the request.
        id (str): Id of the request.
        request (tuple, optional): (method, args, kwargs) retry info, only
            set when the request was sent with `retry=True` (see `retry`).
            Defaults to None.
        queue (str, optional): Simple name of the queue where the request
            was sent. Defaults to None.

    Attributes:
        lost (bool): True when `gather(retry_lost=True)` found out that a
            worker took the request and died before answering. Reset to
            False by `retry`.

    """

    def __init__(
        self,
        rpc_client,
        id: str,
        request: tuple | None = None,
        queue: str | None = None,
    ):
        self._client = rpc_client
        self.id = id
        self._status = PENDING
        self.value: Any = None
        self.error: dict | None = None
        self.creation_time = time()
        self.finished_time: float | None = None
        self.request = request
        self.queue = queue
        self.retries = 0
        self.lost = False
        self.metadata: dict = {}

    def ok(self) -> bool:
        "Returns True if the state of the AsyncResult object is 'OK'."
        return self.status == OK

    def failed(self) -> bool:
        "Returns True if the state of the AsyncResult object is 'FAILED'."
        return self.status == FAILED

    def done(self) -> bool:
        "Returns True if a response was received ('OK' or 'FAILED')."
        return self.ok() or self.failed()

    def pending(self) -> bool:
        """Returns True if the state of the AsyncResult object is 'PENDING'.

        Syncs the object with rpc_client, just in case we have used wait_responses
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory.

        Returns:
            bool: True if 'PENDING', False otherwise
        """
        return self.status == PENDING

    def _raise_exception(self, error: dict) -> None:
        """Raises a RemoteException with the information in error.

        Args:
            error (dict): Dictionary with "code", "message" and/or "trace" as keys
                and a str as value.

        Raises:
            RemoteException
        """
        raise RemoteException(error)

    @property
    def status(self) -> str:
        """Returns the status of the AsyncResult object.

        Syncs the object with rpc_client, just in case we have used wait_responses
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory.

        Returns:
            str: 'PENDING', 'OK' or 'FAILED'
        """
        if self._status == PENDING:
            try:
                self.wait(timeout=0)
            except TimeoutError:
                pass
        return self._status

    def get(self, timeout: float | None = None, clean: bool = True) -> Any:
        """Returns the value of the AsyncResult object.

        Throws a RemoteException exception with the information
        in "error" of the response message.

        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True.

        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        self.wait(timeout, clean)
        if self.ok():
            return self.value
        elif self.failed():
            self._raise_exception(self.error or {})
        raise ValueError("AsyncResult: Undefined Value.")  # shouldn`t happen

    def wait(self, timeout: float | None = None, clean: bool = True) -> None:
        """Waits for result and updates the AsyncResult object.

        Throws TimeoutError if timeout reached.

        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True.

        Raises:
            TimeoutError
        """
        if self._status == PENDING:
            response = self._client.wait_one_response(self.id, timeout, clean=clean)
            if "result" in response:
                self.finished_time = response[
                    "finished_time"
                ]  # Included by Client, not in message
                self._status = OK
                self.value = response["result"]
                self.metadata = response.get("metadata", {})

            elif "error" in response:
                self.finished_time = response[
                    "finished_time"
                ]  # Included by Client, not in message
                self._status = FAILED
                self.error = response["error"]
                self.metadata = response.get("metadata", {})

    def safe_get(
        self,
        timeout: float | None = None,
        clean: bool = True,
        default: Any = None,
    ) -> Any:
        """Like `get`, but returns `default` instead of raising.

        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True.
            default: Value to return on timeout or remote error.
                Defaults to None.

        Returns:
            result, or `default` on any exception.

        """
        try:
            return self.get(timeout, clean=clean)
        except Exception:
            return default

    def retry(self, queue: str | None = None) -> bool:
        """Retry the request associated with this AsyncResult.

        Retries only if the request is still pending. The request must have
        been created with retry=True.

        Args:
            queue (str, optional): Queue to resend the request to. Defaults to
                None, which means the queue of the original request. If given,
                it is used as is (the registry is not consulted); see
                `Client.rpc_async` for the rules.

        Returns:
            bool: True if the request was retried, False otherwise (the request
                had already been received).

        Raises:
            ValueError: If the request was created without retry=True (see
                `Client.rpc_async`).
        """
        if self.request is None:
            raise ValueError(
                "AsyncResult.retry(): no retry information available. The request must be created with retry=True."
            )
        if self.pending():
            if queue is None:
                queue = self.queue
            method, args, kwargs = self.request
            new_id, new_queue = self._client.send_single_request(
                method, args, kwargs, queue=queue, id=self.id
            )
            logger.debug(
                f"{timestamp()} Client: {self._client.client_id} Retrying request with id: {self.id} to queue: {new_queue}"
            )
            self.queue = new_queue
            assert new_id == self.id
            self.retries += 1
            self.lost = False
            return True
        else:
            logger.debug(
                f"{timestamp()} Client: {self._client.client_id} Not Retrying: Response to Request with id: {self.id} already received."
            )
            return False


# Implementation notes.
#
# The AsyncResults are re-filtered with `pending()` on every iteration, and
# only the pending ids go to `Client.wait_responses`. An AsyncResult that
# has already synced its value (with `clean=True`, the default) is no longer
# in the client's `responses` nor `pending` caches, and `wait_responses`
# raises ValueError for such an id.
#
# The clients are waited one after another, not in threads. With a common
# deadline the result is the same: if client A uses up the time, client B
# gets a timeout of ~0 and `wait_responses` still drains its queue with
# `pop_all` before returning. Responses stay in each client's queue while
# another client is being waited on. Threads would only add an executor
# and cross-thread exceptions.
#
# `step` limits how long each `wait_responses` call blocks. The loop
# wakes up after `step` seconds and runs the lost-request check. Without
# `retry_lost`, `step` only changes the number of iterations, not the
# result.
#
# Lost requests, step by step.
#
# 1. A worker takes a request out of the queue and then runs it. There
#    is no list of requests in progress. If the worker dies while it
#    runs the request, the request is gone. Nobody will answer it.
#
# 2. A request that is still in the queue is not lost. A new worker will
#    take it later. We must not resend it: it would run twice.
#
# 3. From the client we cannot look inside a queue. But queues are FIFO.
#    So if a request sent later to the same queue has been answered, the
#    earlier request is no longer in the queue. A worker took it.
#
# 4. We compare times. For an answered request, the worker sends back
#    `metadata.timing.request_sent`: the time when the client built the
#    message. For a pending request we use `Client.pending[id]`: the time
#    of its last send. `retry` updates that time; `creation_time` does
#    not. All the clients in one `gather` call run in the same process,
#    so their clocks are the same and every answered request in `fs` is
#    evidence, no matter which client sent it.
#
# 5. "A worker took it" is not the same as "it is lost". The request may
#    be running now in a worker that is alive. A worker runs one request
#    at a time and takes them in FIFO order. So an alive worker that has
#    answered a request sent later to the same queue cannot be holding
#    the earlier one: it would have taken and answered the earlier one
#    first. Each answer carries `metadata.worker`, so we know who
#    answered what. The request is lost when a later request on its
#    queue was answered and every alive registered worker on that queue
#    answered a request sent after it. With no alive worker on the
#    queue, the second part holds by itself. Dead workers play no part:
#    the rule does not need them to stay in the registry, so pruning at
#    any moment is safe.
#
# 6. The `lost` flag on the AsyncResult remembers the decision, because
#    the request may have to wait for a queue with alive workers. `retry`
#    clears the flag. A resent request needs new evidence, so there is
#    no limit on the number of resends.
#
# 7. Two cases cannot be decided from the client. First, the newest
#    request on a queue: nothing was sent after it, so nothing can prove
#    that a worker took it. Second, an alive worker on the queue that
#    answered nothing sent after the request (idle, slow, or serving
#    other clients): it may be holding the request. Both stay pending.
#    A worker added with `register=False` is not in the registry, so it
#    cannot be excluded: with such workers keep the functions idempotent.
def gather(
    fs: list,
    timeout: float | None = None,
    step: float = 5.0,
    retry_lost: bool = False,
    max_age: float | None = None,
) -> list:
    """Waits for the responses of a list of AsyncResults.

    The AsyncResults may come from different `Client` instances. The wait
    is a loop: every `step` seconds the pending ones are checked and, with
    `retry_lost=True`, the requests taken by a worker that died before
    answering are resent.

    Args:
        fs (list): AsyncResult objects, possibly from different clients.
        timeout (float, optional): Total waiting time in seconds, shared by
            all the clients: it does not add up per client. Defaults to
            None, which waits until every response has arrived.
        step (float): Seconds each call to `Client.wait_responses` blocks
            before control returns to this loop. It is the polling period
            of the lost-request check, not a waiting time: with
            `retry_lost=False` it does not change the result. With N
            clients one iteration lasts up to N * step. Defaults to 5.
        retry_lost (bool): If True, `gather` resends the pending requests
            that a worker took from the queue and did not answer because
            it died. Two conditions must hold. First, a request sent later
            to the same queue, by any client in `fs`, has been answered:
            queues are FIFO, so the pending request is not in the queue
            any more. Second, no alive worker can be holding it: every
            alive registered worker on that queue (see
            `Client.alive_workers`) has answered a request sent after it,
            which a worker that runs one request at a time could not do
            while holding the earlier one. The request is resent with
            `AsyncResult.retry` to a queue with alive workers that serves
            the method: the same queue if it is alive again, otherwise a
            random one. If no such queue exists, a warning is logged once,
            the AsyncResult gets `lost=True` and it is resent when a later
            step finds a queue. Only requests created with `retry=True`
            are resent; the others get `lost=True` and a log line.
            A request still in the queue is never resent. Two cases stay
            pending: the newest request on a queue (nothing was sent after
            it) and a request on a queue where an alive worker answered
            nothing sent after it (it may be holding it). Defaults to
            False.
        max_age (float, optional): Seconds without a heartbeat after which
            a registered worker no longer counts as alive, or three times
            its own interval if that is longer (see
            `Connector.dead_workers`). Workers without heartbeats count as
            alive. Defaults to None: each client uses its connector's
            `default_heartbeat_max_age` (30 s; 61 s on the filesystem).

    Returns:
        list: The AsyncResults still pending when the wait ends. [] means
            every response arrived. Use `AsyncResult.get` or `safe_get` on
            `fs` to read the values.

    Examples:
        fs = [c1.rpc_async("add", [1, 2], retry=True), c2.rpc_async("add", [3, 4], retry=True)]
        gather(fs, timeout=60)                       # [] if both arrived in time
        gather(fs, timeout=60, retry_lost=True)      # resend what a dead worker took
        [f.get() for f in fs]

    """
    deadline = None if timeout is None else time() + timeout
    while True:
        pending = [f for f in fs if f.pending()]
        if not pending:
            return []
        if retry_lost:
            _retry_lost_requests(fs, pending, max_age)
        if deadline is not None and time() >= deadline:
            return pending

        ids_by_client: dict = {}
        for f in pending:
            ids_by_client.setdefault(f._client, []).append(f.id)
        for client, ids in ids_by_client.items():
            wait = step if deadline is None else max(0.0, min(step, deadline - time()))
            client.wait_responses(ids, timeout=wait)


def _answered_by_queue(fs: list) -> dict:
    "(request_sent, worker_id) of every answered request, by queue ref."
    out: dict = {}
    for f in fs:
        if not f.done():
            continue
        m = f.metadata
        queue_ref, worker = m.get("queue"), m.get("worker")
        sent = m.get("timing", {}).get("request_sent")
        if queue_ref is None or worker is None or sent is None:
            continue  # answered by an old worker version, no metadata
        out.setdefault(queue_ref, []).append((sent, worker))
    return out


def _retry_lost_requests(fs: list, pending: list, max_age: float | None) -> None:
    "Marks the pending requests that no alive worker can hold and resends them."
    answered = _answered_by_queue(fs)
    by_client: dict = {}
    for f in pending:
        by_client.setdefault(f._client, []).append(f)

    for client, ars in by_client.items():
        alive = client.alive_workers(max_age)  # refreshes the cache, prunes

        for f in ars:
            sent = client.pending.get(f.id)
            if sent is None:
                continue
            newly = False
            if not f.lost:
                queue_ref = client.connector.get_requests_queue(f.queue)
                # Steps 3 and 5 of the notes: the workers that answered a
                # request sent after this one cannot be holding it.
                later = {w for t, w in answered.get(queue_ref, []) if t > sent}
                holders = [w for w in alive.get(f.queue, []) if w not in later]
                if later and not holders:
                    f.lost = newly = True
                    logger.warning(
                        f"{timestamp()} gather: request {f.id} is lost. A later "
                        f"request on queue {f.queue} was answered and no alive "
                        f"worker can be holding it (alive: {alive.get(f.queue, [])})."
                    )
            if not f.lost:
                continue

            if f.request is None:
                if newly:
                    logger.warning(
                        f"{timestamp()} gather: request {f.id} cannot be resent. "
                        f"It was created without retry=True."
                    )
                continue
            method = f.request[0]
            live = [q for q in client.all_queues_for_method(method) if alive.get(q)]
            if not live:
                log = logger.warning if newly else logger.debug
                log(
                    f"{timestamp()} gather: no alive worker serves {method}. "
                    f"Request {f.id} will be resent when one appears."
                )
                continue
            target = f.queue if f.queue in live else random.choice(live)
            logger.warning(
                f"{timestamp()} gather: resending lost request {f.id} to queue {target}."
            )
            f.retry(queue=target)
