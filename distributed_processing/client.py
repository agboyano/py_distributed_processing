import base64
import logging
import random
import time
from datetime import datetime

import dill

from .async_result import AsyncResult
from .messages import is_ack, is_batch_response, is_single_response, single_request

logger = logging.getLogger(__name__)


def timestamp():
    return datetime.now().isoformat()


def serialize_python_call(fn, args=[], kwargs={}):
    pickled_fn = dill.dumps(fn)
    # Decode ascii necesario para que no de error de bytes object no serializable
    return [base64.b64encode(pickled_fn).decode("ascii"), args, kwargs]


class Client:
    def __init__(
        self,
        serializer,
        connector,
        client_id=None,
        check_registry="cache",
        use_reply_to=False,
        default_queue="default",
        timeout=24 * 60 * 60,
    ):
        self.serializer = serializer
        self.connector = connector

        self.use_reply_to = use_reply_to

        # Cache for pending responses with id.
        # ids as keys and time() of message creation as values.
        self.pending = {}
        # Cache for responses with id.
        # ids as keys and deserialized responses as values.
        self.responses = {}
        # Cache for notifications (recieved messages with no id)
        # Deserialized responses as values.
        self.notifications = []
        # Cache for responses that failed to be deserialized.
        self.responses_parse_errors = []
        # Cache for acks
        self.acks = {}
        # Used responses (wait_one_response).
        self.responses_used = set()

        self.check_registry = check_registry
        self._registry = {}

        if check_registry == "cache":
            self.update_registry_cache()

        self.client_id = (
            client_id if client_id is not None else self.connector.get_client_id()
        )
        logger.info(f"Client with id: {self.client_id}")

        self.responses_queue = self.connector.get_responses_queue(self.client_id)
        logger.info(f"Results queue: {self.responses_queue}")
        logger.debug(
            f"{timestamp()} Client: {self.client_id} with responses queue: {self.responses_queue} connected"
        )

        self.last_request_idnumber = 0
        self.last_request_id = None

        self.set_default_queue(default_queue)

        self.timeout = timeout

    def set_default_queue(self, queue):
        self.default_queue_ref = self.connector.get_requests_queue(queue)

    def to_requests_queue_ref(self, queue_name):
        return self.connector.get_requests_queue(queue_name)
    
    def simple_queue_name(self, queue_ref):
        return self.connector.requests_queue_name(queue_ref)

    def generate_id(self):
        self.last_request_idnumber += 1
        self.last_request_id = f"{self.client_id}:{str(self.last_request_idnumber)}"
        return self.last_request_id

    def update_registry_cache(self):
        self._registry["methods"] = self.connector.methods_registry()
        self._registry["workers"] = self.connector.workers_registry()

    def registry(self, update=False):
        registry = {}
        if update:
            self.update_registry_cache()

        if "methods" in self._registry:
            registry["methods"] = {}
            for method in self._registry["methods"]:
                registry["methods"][method] = [self.simple_queue_name(x) for x in self._registry["methods"][method]]

        if "workers" in self._registry:
            registry["workers"] = {}
            for queue_ref in self._registry["workers"]:
                registry["workers"][self.simple_queue_name(queue_ref)] = self._registry["workers"][queue_ref]
        
        return registry

    def _all_queue_refs_for_method(self, method):
        if self.check_registry == "always":
            return self.connector.all_queues_for_method(method)
        elif self.check_registry == "cache":
            if method not in self._registry["methods"]:
                return []
            return self._registry["methods"][method]
        else:
            return [self.default_queue_ref]

    def all_queues_for_method(self, method, update=False):
        if update and self.check_registry=="cache":
            self.update_registry_cache()
        return [self.simple_queue_name(x) for x in self._all_queue_refs_for_method(method)]
    
    def all_workers_for_method(self, method, update=False):
        if update or self.check_registry=="always":
            self.update_registry_cache()      
        r = self._registry
        queues = r["methods"][method]
        ws = set()
        for q in queues:
            ws = sorted(list(ws.union(set(r["workers"][q]))))

        return ws

    def _select_queue_ref(self, method):
        """Selects a queue where the request with the method is going to be sent.

        Selects the queue based on:
                 - If client's `check_registry` is 'always', calls the `random_queue_for_method` of the
                     connector instance (connector attribute of the client's instance).
                 - If client's `check_registry` is 'cache', choose a random queue from the available
                     queues for the method based on the information available in the client's cache.
                     The information should be updated with the `update_registry_cache` method.
                 - Client's `default_requests_queue` attribute otherwise.

         Args:
             method (str): method to request.

         Returns:
             str: Queue where the request with the method is going to be sent.

        """
        if self.check_registry == "always":
            queue_ref = self.connector.random_queue_for_method(method)
            if queue_ref is None:
                raise ValueError(f"Method {method} does not exist/is not available.")

        elif self.check_registry == "cache":
            if method not in self._registry["methods"]:
                # If `update_registry` is called every time, 'cache' is,
                # in practice, equivalent to 'always'.
                self.update_registry_cache()
                if method not in self._registry["methods"]:
                    raise ValueError(
                        f"Method {method} does not exist/is not available."
                    )

            available = self._registry["methods"][method]
            queue_ref = random.choice(available)

        else:
            queue_ref = self.default_queue_ref

        return queue_ref

    def send_single_request(
        self,
        method,
        args=None,
        kwargs=None,
        queue=None,
        id=None,
        reply_to=None,
        ack=None,
        is_notification=False,
        **options,
    ):
        """Sends a single RPC `request`.

        If no `id` is provided and `is_notification` is False, generates a new one.
        Reusing an `id` constitutes a retry; the first response that is available
        will be used (with no guaranties).

        Args:
            method (str): Remote function name.
            args (list, optional): Positional arguments for the remote function. Defaults to None.
            kwargs (dict, optional): Keyword arguments for the remote function. Defaults to None.
            id (str, optional): Request identifier. Defaults to None. If None, generates a new `id`.
                If `is_notification` is True, `id` is not defined.
            reply_to (str, optional): Response queue name to be added to the `request` message as the
                `reply_to` key. Defaults to None. Not included in the JSON RPC 2.0 specification.
                If None and the Client's `use_reply_to` is True, uses the Client's `responses_queue` attribute.
                Doesn't set `reply_to` otherwise. If `reply_to` is not defined in the `request`
                message, the worker can response guessing the `response_queue` from de `request` `id`.
            queue (str, optional): Queue to send the request to. Defaults to None. If None, selects
                the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.
            is_notification (bool): True if is a `notification` (a `request` with no `id`).
                Defaults to False.
            **options: Additional arguments added to the RPC message under the 'options' key.

        Returns:
            tuple[str, str]: A tuple containing (request `id`, queue name)

        """
        if queue is not None:
            queue_ref = self.connector.get_requests_queue(queue)
        else:
            queue_ref = self._select_queue_ref(method)

        if not is_notification:
            id_ = self.generate_id() if id is None else id
        else:
            id_ = None

        if reply_to is None:
            reply_to = None if not self.use_reply_to else self.responses_queue

        sr = single_request(
            method,
            args=args,
            kwargs=kwargs,
            id=id_,
            reply_to=reply_to,
            ack=ack,
            is_notification=is_notification,
            **options,
        )

        serialized_sr = self.serializer.dumps(sr)

        self.connector.enqueue(queue_ref, serialized_sr)
        logger.debug(
            f"{timestamp()} Client: {self.client_id} sent request with id: {id_} to queue: {queue_ref}"
        )

        self.pending[id_] = time.time()
        return id_, self.simple_queue_name(queue_ref)

    def send_batch_request(
        self, requests_lst, queue=None, retry=None, ack=None, **options
    ):
        """Sends an batch request that will be executed by a single worker.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs), ...].
                The tuples match the first tree positional args of the `single_request`
                method. They can have less than three items. In this case, they will
                use the default values for the `single_request` args that are not in the tuple.
            queue (str, optional): Queue to send the batch request to. Defaults to None.
                If None, selects randomly one of the common queues available for all the methods
                in `requests_lst`.
            retry (bool): Include requests info in the AsyncResult objects in
                order to make posible retrying every individual request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.
            **options: Additional arguments added to the each individual request under the 'options' key.

        Returns:
            list(str): List of ids of the individual sent requests.

        Raises:
            ValueError: If there is no common queue for all the methods or if `queue` is not None and not included
                in the list of common queues.

        """
        queue_refs_sets = [set(self._all_queue_refs_for_method(x[0])) for x in requests_lst]

        requests_queue_refs = list(set.union(*queue_refs_sets))

        if len(requests_queue_refs) == 0:
            raise ValueError("No common queue for batch request.")

        if queue is not None:
            queue_ref = self.connector.get_requests_queue(queue)
            if queue_ref not in requests_queue_refs:
                raise ValueError(
                    f"{queue} not in common available queues for batch request."
                )
        else:
            if len(requests_queue_refs) >0:
                queue_ref = random.choice(requests_queue_refs)
            else:
                queue_ref = self.default_queue_ref

        reply_to = None if not self.use_reply_to else self.responses_queue

        batch_request = [
            single_request(
                t[0],
                args=t[1],
                kwargs=t[2],
                id=self.generate_id(),
                is_notification=False,
                reply_to=reply_to,
                **options,
            )
            for t in requests_lst
        ]

        serialized_br = self.serializer.dumps(batch_request)

        self.connector.enqueue(queue_ref, serialized_br)

        ids = [t["id"] for t in batch_request]
        logger.debug(
            f"{timestamp()} Client: {self.client_id} sent batch request with {len(ids)} requests to queue: {queue_ref}"
        )

        for id in ids:
            self.pending[id] = time.time()

        return ids

    def _responses_to_dicts(self, raw_responses):
        """Deserialize responses.

        Args:
            raw_responses (list): List of responses (serialized), usually from pop or pop_all.

        Returns:
            tuple [dict, list, list, dict]: (results_dict, no_id, parse_errors, acks_dict)

            results_dict (dict): Dictionary with the ids of the request as keys
                and the deserialized response as value. The deserialized response
                is a dict with either the key "result" or "error". The get method
                of the AsyncResult instance, associated with the id, returns the "result",
                if available, or throws an exception with the information in "error".
            no_id (list): List with all the deserialized responses that have no id (notifications).
            parse_errors (list): List with all the responses that failed to be deserialized.
            acks_dict (dict):  Dictionary with the ids of the request as keys
                and the deserialized ACKS as value.

        """
        results_dict = {}
        acks_dict = {}
        no_id = []
        parse_errors = []

        for e in raw_responses:
            try:
                r = self.serializer.loads(e)
            except:
                parse_errors.append(e)
                logger.debug(
                    f"{timestamp()} Client: {self.client_id} a Message could NOT be deserialized"
                )
                continue

            if is_batch_response(r):  # Batch response. Not implemented in worker.
                logger.debug(
                    f"{timestamp()} Client: {self.client_id} received a Batch Response with {len(r)} items"
                )
                for rr in r:
                    rr["finished_time"] = time.time()
                    if "id" in rr:
                        results_dict[rr["id"]] = rr
                        logger.debug(
                            f"{timestamp()} Client: {self.client_id} processed a {'RESULT' if 'error' not in rr else 'ERROR'} with id: {rr['id']} from BATCH response"
                        )

                    else:
                        logger.debug(
                            f"{timestamp()} Client: {self.client_id} processed a Notification from BATCH response"
                        )
                        no_id.append(rr)

            elif is_single_response(r):
                r["finished_time"] = time.time()
                if "id" in r:
                    results_dict[r["id"]] = r
                    logger.debug(
                        f"{timestamp()} Client: {self.client_id} received a Single {'RESULT' if 'error' not in r else 'ERROR'} with id: {r['id']}"
                    )
                else:
                    logger.debug(
                        f"{timestamp()} Client: {self.client_id} received a Single Notification"
                    )
            elif is_ack(r):
                r = r["ack"]
                acks_dict[r["id"]] = r
                logger.debug(
                    f"{timestamp()} Client: {self.client_id} received an ACK from worker: {r['worker']} for id: {r['id']}"
                )

            else:
                logger.debug(
                    f"{timestamp()} Client: {self.client_id} a Message could NOT be processed"
                )

        return results_dict, no_id, parse_errors, acks_dict

    def _update_responses_cache(self, raw_responses):
        """Deserialize raw_responses and update caches.

        Updates the client caches responses, notifications, responses_parse_errors and pending.

        Args:
            raw_responses (list): List of responses (serialized), usually from pop or pop_all.
        """
        responses_dict, no_id, parse_errors, acks_dict = self._responses_to_dicts(
            raw_responses
        )
        self.responses.update(responses_dict)
        self.notifications.append(no_id)
        self.responses_parse_errors.append(parse_errors)
        self.acks.update(acks_dict)
        pending = [k for k in self.pending.keys()]
        for id in pending:
            if id in self.responses:
                del self.pending[id]

        acks = [k for k in self.acks.keys()]
        for id in acks:
            if id in self.responses:
                del self.acks[id]

    def _update_cache_with_all_available_responses(self):
        all_responses = self.connector.pop_all(self.responses_queue)
        self._update_responses_cache(all_responses)

    def wait_responses(self, ids=None, timeout=None):
        """Wait for all responses in ids.

        If ids is None, waits for all pending ids.

        Args:
            ids (list, optional): List of ids. Defaults to None.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            list: Pending ids if timeout, [] if ok.

        Raises:
            TimeoutError
            ValueError: If there are ids neither in responses nor in pending.
        """
        if timeout is None:
            timeout = self.timeout

        if ids is None:
            ids = [k for k in self.pending.keys()]
        else:
            tmp = [k for k in ids if k not in self.responses and k not in self.pending]
            if len(tmp) > 0:
                raise ValueError(
                    f"wait_responses: {tmp} neither in responses nor in pending."
                )

        pending = [k for k in ids if k not in self.responses]

        if len(pending) > 0:
            self._update_cache_with_all_available_responses()
        else:
            return []

        pending = [k for k in pending if k not in self.responses]

        if timeout is None:
            forever, time_left = True, -1

        else:
            t_0 = time.time()
            forever, time_left = False, timeout

        while (len(pending) > 0) and ((time_left > 0.000001) or forever):
            next_resp = self.connector.pop(self.responses_queue, timeout=time_left)

            if (
                next_resp is not None
            ):  # if None then timeout, if not (queue_name, value)
                self._update_responses_cache([next_resp[1]])

            if not forever:
                time_left = timeout - (time.time() - t_0)

            pending = [k for k in pending if k not in self.responses]

        return pending

    def wait_one_response(self, id, timeout=None, clean=True):
        """Wait for the response with id=id.

        Used by de get method of AsyncResult.

        Args:
            id (str):
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True.

        Returns:
            dict: Response deserialized, with either the keys "result" or "error".
                The get method of the AsyncResult instance, associated with the id,
                returns the "result", if available, or throws an exception with the
                information in "error".

        Raises:
            TimeoutError
            ValueError: If id neither in responses nor in pending.
        """
        if len(self.wait_responses([id], timeout)) > 0:
            raise TimeoutError()

        response = self.responses[id]
        self.responses_used.add(id)

        if clean:
            del self.responses[id]

        return response

    def clean_used(self):
        """Clean all responses that have been used at least once."""
        responses = [k for k in self.responses]
        for id in responses:
            if id in self.responses_used:
                del self.responses[id]

    def rpc_async(self, method, args=[], kwargs={}, queue=None, retry=False, ack=None):
        """Sends an asynchronous single request.

        Args:
            method (str): Remote function name.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.
            retry (bool): Include requests info in AsyncResult object in
                order to make posible retrying the request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            AsyncResult
        """
        request = (method, args, kwargs) if retry else None
        id, queue = self.send_single_request(method, args, kwargs, queue=queue, ack=ack)
        return AsyncResult(self, id, request, queue)

    def rpc_sync(self, method, args=[], kwargs={}, queue=None, timeout=None):
        """Sends an asynchronous single request.

        Args:
            method (str): Remote function name.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        return self.rpc_async(method, args, kwargs, queue).get(timeout)

    def rpc_batch_async(self, requests_lst, queue=None, retry=False, ack=None):
        """Sends an asynchronous batch request that will be executed by a single worker.

        Each individual request within the batch has is own id assigned.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs, queue), ...].
                The tuples match the first four positional args of the `rpc_async`
                method. They can have less than four items. In this case, they will
                use the default values for the `rpc_async` args that are not in the tuple.
            queue (str, optional): Queue to send the batch request to. Defaults to None.
                If None, selects randomly one of the common queues available for all the methods
                in `requests_lst`.
            retry (bool): Include requests info in the AsyncResult objects in
                order to make posible retrying every individual request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received.
                False or None otherwise. Defaults to None.

        Returns:
            list: List of AsyncResult objects of the individual requests within the batch.

        """
        ids = self.send_batch_request(requests_lst, queue=queue, retry=retry, ack=ack)
        return [AsyncResult(self, id) for id in ids]

    def rpc_batch_sync(self, requests_lst, timeout=None):
        """Sends a synchronous batch request that will be executed by a single worker.

        Waits for the results.
        Uses safe_get, if there's an error in a function, returns None.

        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...]
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            list: List of (results or None on error)

        Raises:
            TimeoutError

        """
        fs = self.rpc_batch_async(requests_lst)
        return [f.safe_get(timeout=timeout) for f in fs]

    def rpc_multi_async(self, requests_lst, retry=False, ack=None):
        """Sends multiple asynchronous requests that will be distributed among workers.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs, queue), ...].
                The tuples match the first four positional args of the `rpc_async`
                method. They can have less than four items. In this case, they will
                use the default values for the `rpc_async` args that are not in the tuple.
            retry (bool): Include requests info in the AsyncResult objects in
                order to make posible retrying every individual request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            list: List of AsyncResult objects.

        """
        return [self.rpc_async(*t[:], retry=retry, ack=ack) for t in requests_lst]

    def rpc_multi_sync(self, requests_lst, timeout=None):
        """Sends multiple synchronous requests that will be distributed among workers.

        Waits for the results.
        Uses safe_get, if there's an error in a function, returns None.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs, queue), ...].
                The tuples match the first four positional args of the `rpc_async`
                method. They can have less than four items. In this case, they will
                use the default values for the `rpc_async` args that are left out of the tuple.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            list: List of (results or None on error).

        Raises:
            TimeoutError

        """
        fs = self.rpc_multi_async(requests_lst, retry=False)
        return [f.safe_get(timeout=timeout) for f in fs]

    def rpc_async_fn(self, fn, args=[], kwargs={}, queue=None, retry=False, ack=None):
        """Sends an asynchronous single request with a local python function.

        Args:
            fn (function): Local function to be serialized and sent.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.
            retry (bool): Include requests info in AsyncResult object in
                order to make posible retrying the request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            AsyncResult:

        Raises:
            TimeoutError
            RemoteException

        """
        py_call = serialize_python_call(fn, args=args, kwargs=kwargs)
        method, args = "eval_py_function", py_call
        request = (method, args, None) if retry else None
        id, queue = self.send_single_request(method, args=args, queue=queue, ack=ack)
        return AsyncResult(self, id, request, queue)

    def rpc_sync_fn(self, method, args=[], kwargs={}, queue=None, timeout=None):
        """Sends a synchronous single request with a local python function.

        Args:
            fn (function): Local function to be serialized and sent.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If None, selects the queue based on:
                - Available queues for the method if client's `check_registry` is 'always' or 'cache'
                - Client's `default_requests_queue` attribute otherwise.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            result

        Raises:
            TimeoutError
            RemoteException

        """
        return self.rpc_async_fn(method, args, kwargs, queue).get(timeout)
