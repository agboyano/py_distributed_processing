from __future__ import annotations

import logging
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
        """Retries the request linked to the AsyncResult instance.

        Only retries if the request is pending.

        Args:
            queue (str, optional): queue to sent the request. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.

        Returns:
            bool: True if the request has been retried, False if not (request already received).

        Raises:
            ValueError: If the AsyncResult was created without retry info
                (see `Client.rpc_async(retry=True)`).

        """
        if self.request is None:
            raise ValueError(
                "AsyncResult.retry(): request info is None. Can not retry."
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


def gather(
    fs: list,
    timeout: float | None = None,
    step: float = 5,
    max_dt: float = 30,
) -> bool:
    """Gathers all AsyncResults in the list.

    Assumes all requests were sent by the same client. If no progress is
    made for a while (relative to the longest observed execution time),
    requests that look lost are retried once (only those created with
    retry info, see `Client.rpc_async(retry=True)`).

    Args:
        fs (list): List of AsyncResult objects.
        timeout (int, float, optional): Timeout in seconds. Maximum waiting time.
            Defaults to None. If None, unlimited waiting time.
        step (int, float): Step time in seconds. Defaults to 5 secs.
        max_dt (int, float): Max delta time to retry a request.

    Returns:
        bool: True if all responses were received, False if timeout.
    """

    def ar_indices_to_retry(fs):
        # Queues are FIFO: if a request older than the last completed one
        # in the same queue is still pending, it was probably lost.
        by_queue = {}
        for i, f in enumerate(fs):
            by_queue.setdefault(f.queue, []).append((i, f.done()))

        indices = []
        for entries in by_queue.values():
            last_done = -1
            for pos, (_, done) in enumerate(entries):
                if done:
                    last_done = pos
            for i, done in entries[: last_done + 1]:
                if not done:
                    indices.append(i)

        return indices

    _ = [f.status for f in fs]
    # AsyncResults sorted by creation_time
    fs = sorted(fs, key=lambda f: f.creation_time)

    N = len(fs)
    t_0 = time()
    last_progress = t_0
    i = 0
    N_pending = N
    max_dt_real = max_dt
    retried = set()
    while timeout is None or (time() - t_0) <= timeout:
        pending_ars = [ar for ar in fs if not ar.done()]
        pending_ids = [ar.id for ar in pending_ars]

        if len(pending_ars) == 0:
            return True

        if len(pending_ars) < N_pending:
            N_pending = len(pending_ars)
            last_progress = time()

        dts = [
            f.metadata["timing"]["execution_finish"]
            - f.metadata["timing"]["execution_start"]
            for f in fs
            if f.done()
            and "execution_start" in f.metadata.get("timing", {})
            and "execution_finish" in f.metadata.get("timing", {})
        ]

        if len(dts) > 0:
            max_dt_real = max(max(dts), max_dt)

        if (time() - last_progress) > 2 * max_dt_real:
            for ix in ar_indices_to_retry(fs):
                if ix not in retried and fs[ix].request is not None:
                    fs[ix].retry()
                    retried.add(ix)
                    logger.debug(f"Retrying request with id: {fs[ix].id}")
            logger.warning(
                "gather: no progress for a while. It looks like there are no workers on the other side."
            )

        logger.debug(
            f"{i}: seconds {time() - t_0}s, AR recovered {N - N_pending}, AR left {N_pending}, max delta {max_dt_real}"
        )

        try:
            if (
                fs[0]._client.wait_responses(pending_ids, timeout=step) == []
            ):  # assumes all requests were sent by the same client
                return True
        except TimeoutError:
            pass

        _ = [f.status for f in fs]

        i += 1

    return False
