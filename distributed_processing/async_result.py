from __future__ import annotations

import logging
import random
from datetime import datetime
from time import time
from typing import Any

from .connector import DEFAULT_HEARTBEAT_MAX_AGE
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
# `step` bounds each `wait_responses` call so that the loop wakes up and
# can run the dead-queue check; without `retry_dead` it only changes how
# many iterations happen, not the outcome.
#
# A request is resent at most once (`retries > 0` is skipped). "No alive
# worker" is read from `Client.alive_workers`: a queue missing from the
# registry (pruned, or never registered) or present with an empty list. A
# queue added with `register=False` therefore always looks dead: its
# request is resent once to the same queue. Resending a request that was
# still in the queue duplicates it if a worker comes back, hence the
# idempotency note in the docstring.
def gather(
    fs: list,
    timeout: float | None = None,
    step: float = 5.0,
    retry_dead: bool = False,
    max_age: float = DEFAULT_HEARTBEAT_MAX_AGE,
) -> list:
    """Waits for the responses of a list of AsyncResults.

    The AsyncResults may come from different `Client` instances. The wait
    is a loop: every `step` seconds the pending ones are checked and, with
    `retry_dead=True`, the requests stuck in a queue without alive workers
    are resent.

    Args:
        fs (list): AsyncResult objects, possibly from different clients.
        timeout (float, optional): Total waiting time in seconds, shared by
            all the clients: it does not add up per client. Defaults to
            None, which waits until every response has arrived.
        step (float): Seconds each call to `Client.wait_responses` blocks
            before control returns to this loop. It is the polling period
            of the dead-queue check, not a waiting time: with
            `retry_dead=False` it does not change the result. With N
            clients one iteration lasts up to N * step. Defaults to 5.
        retry_dead (bool): If True, a pending request whose queue has no
            alive worker (see `Client.alive_workers`) is resent once with
            `AsyncResult.retry`: to a queue with alive workers that serves
            the method if there is one, otherwise to the same queue. Only
            requests created with `retry=True` can be resent. If the
            request was still in the queue and a worker comes back, it
            runs twice, so the function must be idempotent. A queue added
            with `register=False` is not in the registry and always looks
            dead. Defaults to False.
        max_age (float): Seconds without a heartbeat after which a worker
            counts as dead, passed to `Client.alive_workers`. Workers
            without heartbeats count as alive. Defaults to
            `DEFAULT_HEARTBEAT_MAX_AGE` (30 s).

    Returns:
        list: The AsyncResults still pending when the wait ends. [] means
            every response arrived. Use `AsyncResult.get` or `safe_get` on
            `fs` to read the values.

    Examples:
        fs = [c1.rpc_async("add", [1, 2], retry=True), c2.rpc_async("add", [3, 4], retry=True)]
        gather(fs, timeout=60)                       # [] if both arrived in time
        gather(fs, timeout=60, retry_dead=True)      # resend what sits on a dead queue
        [f.get() for f in fs]

    """
    deadline = None if timeout is None else time() + timeout
    while True:
        pending = [f for f in fs if f.pending()]
        if not pending:
            return []
        if retry_dead:
            _retry_on_dead_queues(pending, max_age)
        if deadline is not None and time() >= deadline:
            return pending

        ids_by_client: dict = {}
        for f in pending:
            ids_by_client.setdefault(f._client, []).append(f.id)
        for client, ids in ids_by_client.items():
            wait = step if deadline is None else max(0.0, min(step, deadline - time()))
            client.wait_responses(ids, timeout=wait)


def _retry_on_dead_queues(pending: list, max_age: float) -> None:
    "Resends, once, the pending requests whose queue has no alive worker."
    by_client: dict = {}
    for f in pending:
        by_client.setdefault(f._client, []).append(f)

    for client, ars in by_client.items():
        alive = client.alive_workers(max_age)  # refreshes the registry cache too
        for f in ars:
            if f.request is None or f.retries > 0 or alive.get(f.queue):
                continue
            method = f.request[0]
            live_queues = [
                q for q in client.all_queues_for_method(method) if alive.get(q)
            ]
            target = random.choice(live_queues) if live_queues else None
            logger.warning(
                f"{timestamp()} gather: queue {f.queue} has no alive worker. "
                f"Retrying request {f.id} on queue {target or f.queue}."
            )
            f.retry(queue=target)
