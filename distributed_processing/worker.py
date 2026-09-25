from __future__ import annotations

import base64
import inspect
import logging
import random
import threading
from datetime import datetime
from time import sleep, time
from typing import Any, Callable

import dill

from .connector import MISSING, Connector
from .messages import (
    ack,
    error_response,
    is_batch_request,
    is_batch_response,
    is_single_request,
    is_single_response,
    result_response,
)


def timestamp() -> str:
    return datetime.now().isoformat()


logger = logging.getLogger(__name__)


# Implementation notes.
#
# Counterpart of `client.serialize_python_call`. The client pickles the
# function with dill and encodes the bytes in base64, because the request
# params go through the connector's serializer and the default one on Redis
# is JSON, which cannot carry bytes. So `str_fn` arrives as an ASCII str and
# has to be base64-decoded before `dill.loads`. dill is required on this
# side too: functions defined in a notebook or in `__main__` are pickled by
# value and only dill can rebuild them; functions imported from a module
# are pickled by reference and that module must be importable here, at a
# version compatible with the client's.
#
# `args` and `kwargs` are the arguments of the *inner* call. They come as
# positional params of the request, so the worker's signature check only
# validates this function's three parameters; a mismatch with the user's
# function surfaces as an internal error (-32603) with the remote traceback.
#
# SECURITY: this executes arbitrary code sent by clients. It is only
# available on queues added with `Worker.add_python_eval`.
def eval_py_function(
    str_fn: str, args: list | None = None, kwargs: dict | None = None
) -> Any:
    "Evals a dill-serialized function. str_fn is encoded in base64 ascii."
    args = [] if args is None else args
    kwargs = {} if kwargs is None else kwargs
    return dill.loads(base64.b64decode(str_fn))(*args, **kwargs)


def check_params(fn: Callable, args: list, kwargs: dict) -> None:
    """Raises TypeError if `args`/`kwargs` do not match the signature of `fn`.

    Lets the worker tell "invalid params" (-32602) apart from a TypeError
    raised *inside* the function (-32603). If the signature of `fn` cannot
    be inspected (some builtins and C extensions), the check is skipped.
    """
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return
    signature.bind(*args, **kwargs)


class Worker:
    """Listens on requests queues, executes registered functions and responds.

    Queues are added with `add_requests_queue`/`add_function` and made public
    with `update_methods_registry`. `run` (or `run_once`) pops requests from
    the queues in priority order and sends the responses back through the
    connector. Use `close()` or a `with` block to unregister the worker's
    public queues and methods on shutdown.

    Args:
        connector (Connector): Transport instance (e.g. `RedisConnector`,
            `FileSystemConnector`). Messages are handed to the connector as
            Python objects; the connector owns the wire encoding.
        worker_id (str, optional): Worker identifier. Defaults to None.
            If None, a new one is requested to the connector.
        with_trace (bool): If True, error responses include the remote
            traceback in the "trace" key of the error object.
            Defaults to True.
        reply_to_default (str, optional): Default response queue, only used
            when the incoming request has no `reply_to` key. Defaults to
            None (derive the response queue from the request `id`).
        heartbeat_interval (float, optional): Seconds between heartbeats
            while `run` or `run_forever` is running (see `start_heartbeat`).
            Defaults to 10. None disables heartbeats. The interval is sent
            with each heartbeat: a client counts the worker dead after
            three missed beats, or after its `max_age` (30 s by default)
            if that is longer.

    """

    def __init__(
        self,
        connector: Connector,
        worker_id: str | None = None,
        with_trace: bool = True,
        reply_to_default: str | None = None,
        heartbeat_interval: float | None = 10.0,
    ):
        self.connector = connector
        self.heartbeat_interval = heartbeat_interval
        self._heartbeat_thread: threading.Thread | None = None
        self._heartbeat_stop = threading.Event()

        # Insertion order defines queue priority (dicts respect key
        # insertion order since Python 3.7).
        # Queue refs as keys, ({method_name: function, ...}, priority)
        # tuples as values.
        self.requests_queues: dict = {}

        # Some queues may be kept out of the public registry.
        self.queues_to_register: set = set()

        # If with_trace is True, remote exceptions include the traceback.
        self.with_trace = with_trace

        # Optional default response queue. Only used when the incoming
        # request has no "reply_to" key. If None, the "reply_to" is derived
        # from the request "id".
        self.reply_to_default = reply_to_default
        self.worker_id = (
            worker_id if worker_id is not None else self.connector.get_server_id()
        )
        self._closed = False
        self._stop_requested = False
        logger.info(f"Worker id: {self.worker_id}")

    def add_requests_queue(
        self,
        simple_queue_name: str,
        func_dict: dict,
        priority: int = 10,
        register: bool = True,
    ) -> None:
        """Add queue with functions.

        If `register` is True, the functions and their queues are made public
        with the Worker's `update_methods_registry` method.

        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received.
            func_dict (dict): Dictionary with public method names as keys
                and functions as values {"func_name":func, ...}.
            priority (int): Priority of the queue. Defaults to 10.
                Queues with the same priority will be shuffled every time
                the worker checks for a new request.
            register (bool): Defaults to True. If False, functions (methods)
                and their queues are not made public, but remain available
                anonymously. This prevents exposing queues that may contain
                many methods or should remain private.

        """
        # Generate internal queue name requests_{queue_name} from queue_name
        requests_queue = self.connector.get_requests_queue(simple_queue_name)
        self.requests_queues[requests_queue] = (func_dict, priority)
        if register:
            self.queues_to_register.add(requests_queue)
        # remember to call self.update_methods_registry() afterwards

    def add_function(
        self,
        simple_queue_name: str,
        fn_name: str,
        fn: Callable,
        priority: int = 10,
        register: bool = True,
    ) -> None:
        """Add a function to a queue.

        If `register` is True, and the queue doesn't exist, the function and the queue
        will be public when the Worker's `update_methods_registry` method is used.

        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received.
            fn_name (str): Method name to be available for the queue.
            priority (int): Priority of the queue. Defaults to 10.
                Queues with the same priority will be shuffled every time
                the worker checks for a new request.
            register (bool): Defaults to True. If False, the method
                and the queue are not made public, but remain available
                anonymously.

        """
        requests_queue = self.connector.get_requests_queue(simple_queue_name)

        if requests_queue not in self.requests_queues:
            self.add_requests_queue(
                simple_queue_name, {fn_name: fn}, priority, register
            )
        else:
            self.requests_queues[requests_queue][0][fn_name] = fn
        # remember to call self.update_methods_registry() afterwards

    def add_python_eval(
        self,
        simple_queue_name: str = "py_eval",
        priority: int = 20,
        register: bool = True,
    ) -> None:
        """Adds a queue with the method `eval_py_function`, that evals a serialized python function.

        SECURITY WARNING: `eval_py_function` executes arbitrary Python code
        sent by clients. Only use it on trusted infrastructure.

        If `register` is True, `eval_py_function` and the queue are made public
        with the Worker's `update_methods_registry` method.

        Equivalent to:

            server.add_requests_queue(
                "py_eval", {"eval_py_function": eval_py_function}
            )

        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received. Defaults to 'py_eval'.
            priority (int): Priority of the queue. Defaults to 20.
                Queues with the same priority will be shuffled every time
                the worker checks for a new request.
            register (bool): Defaults to True. If False, `eval_py_function`
                and the queue are not made public, but remain available
                anonymously.

        """
        self.add_function(
            simple_queue_name, "eval_py_function", eval_py_function, priority, register
        )

    def add_requests_queues(self, queues: dict, register: bool = True) -> None:
        """Add the information in the queues dict to the registry.

        If `register` is True, the functions and their queues are made public
        with the Worker's `update_methods_registry` method.

        At the moment the queues are checked in order.

        TO DO: Adding priority and normal queues (to be shuffle).

        Args:
            queues (dict): Dictionary with the same structure as the Worker's
                requests_queues attribute. Unlike the dictionary in the
                Worker's requests_queues attribute, the keys in queues
                represent the simple names of the queues rather than
                their long names. The corresponding long name for each queue
                is retrieved using get_requests_queue(simple_queue_name).
                Structure:
                    {simple_queue_name:({method_name:function, ...}, priority), ...}
            register (bool): Defaults to True. If False, functions (methods)
                and their queues are not made public, but remain available
                anonymously.

        """
        for simple_queue_name in queues:
            self.add_requests_queue(
                simple_queue_name,
                queues[simple_queue_name][0],
                priority=queues[simple_queue_name][1],
                register=register,
            )
        # remember to call self.update_methods_registry() afterwards

    # ---- shared variables (delegated to the connector) -----------------------

    def set_variable(self, name: str, value: Any) -> None:
        """Stores a variable shared by every client and worker of the namespace.

        Same as `connector.set_variable`. The value goes through the
        connector's serializer and the last write wins.

        Args:
            name (str): Variable name.
            value: Python object to share.

        """
        self.connector.set_variable(name, value)

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Returns a copy of a shared variable, or `default` if it is not set.

        Same as `connector.get_variable`. A registered function does not
        receive the worker, so it reads variables through a closure:

            worker.add_function("q", "price", lambda isin: price(isin, worker.get_variable("valuation_date")))

        """
        return self.connector.get_variable(name, default)

    def update_variable(self, name: str, fn: Callable, default: Any = MISSING) -> Any:
        """Atomically replaces a shared variable with `fn(current)`; returns the new value.

        Same as `connector.update_variable`: the read-modify-write runs
        under a per-variable lock, so several workers counting on the same
        variable do not lose updates. Keep `fn` pure and quick. A missing
        variable raises `KeyError` unless `default` is given.
        """
        return self.connector.update_variable(name, fn, default)

    def delete_variable(self, name: str) -> bool:
        "Deletes a shared variable. Returns True if it existed."
        return self.connector.delete_variable(name)

    def variables(self) -> list:
        "Returns the sorted names of the shared variables."
        return self.connector.variables()

    def update_methods_registry(self) -> None:
        "Publishes the queues and methods added with `register=True`."
        queues_to_register = {
            k: v[0]
            for (k, v) in self.requests_queues.items()
            if k in self.queues_to_register
        }

        self.connector.register_methods(queues_to_register, self.worker_id)

    # Implementation notes.
    #
    # The heartbeat runs in its own daemon thread, not in the `run_once`
    # loop. The loop blocks in `pop_multiple(timeout=-1)` while there are no
    # requests, and it blocks for the whole execution of a registered
    # function; a worker busy for ten minutes would look dead. Telling
    # "busy" apart from "died in the middle of a task" is the point of the
    # heartbeat, so it must beat while the loop is blocked.
    #
    # The thread only calls `connector.heartbeat`, one write to the value
    # store. redis-py shares a thread-safe connection pool, an `fs_structs`
    # set is one atomic rename and the in-memory connector is a dict, so
    # the thread needs no lock of its own. A transport error is logged and
    # the thread carries on: a hiccup on a shared drive must not kill the
    # worker (`run_forever` has the same policy for the main loop).
    #
    # `Event.wait(interval)` is the sleep: `stop_heartbeat` sets the event
    # and the thread returns at once instead of after the interval.
    #
    # Each beat also sends `heartbeat_interval`. A client cannot know how
    # often this worker beats, so it uses the published interval to give
    # it the right tolerance (see `Connector.dead_workers`).
    #
    # `stop_heartbeat` does not delete the heartbeat. Only `close` does.
    # `run` and `run_forever` stop the thread in their `finally`, and a
    # `KeyboardInterrupt` goes through that `finally` too. If the key were
    # deleted there, a worker stopped with Ctrl-C and never closed would
    # stay in the registry with no heartbeat, which means "alive" forever
    # and impossible to prune. With the last beat left in place, it goes
    # stale and `prune_dead_workers` removes it.
    def start_heartbeat(self) -> None:
        """Starts the heartbeat thread. Idempotent; no-op if disabled.

        Writes one heartbeat immediately and then one every
        `heartbeat_interval` seconds until `stop_heartbeat`. Each heartbeat
        also publishes the interval. `run` and `run_forever` call it for
        you; call it yourself only if you drive the worker with `run_once`.
        """
        if self.heartbeat_interval is None:
            return
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return

        self._heartbeat_stop.clear()
        self.connector.heartbeat(self.worker_id, self.heartbeat_interval)

        def beat():
            while not self._heartbeat_stop.wait(self.heartbeat_interval):
                try:
                    self.connector.heartbeat(self.worker_id, self.heartbeat_interval)
                except Exception:
                    logger.exception(
                        f"{timestamp()} Worker: {self.worker_id} heartbeat failed."
                    )

        self._heartbeat_thread = threading.Thread(
            target=beat, name=f"heartbeat-{self.worker_id}", daemon=True
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        """Stops the heartbeat thread. Idempotent.

        The last heartbeat stays in the store. A worker that stops without
        `close()` goes stale after its tolerance and can be pruned with
        `prune_dead_workers`. `close()` deletes the heartbeat.
        """
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None:
            thread.join(timeout=5)
            self._heartbeat_thread = None

    def unregister(self) -> None:
        "Removes the worker's queues and methods from the public registry."
        logger.debug(f"{timestamp()} Worker: {self.worker_id} unregistering.")
        self.connector.unregister_methods(self.worker_id)

    def close(self) -> None:
        """Stops the heartbeat, unregisters the worker and deletes its heartbeat. Idempotent.

        After `close()` the worker is gone from the registry and from
        `alive_workers` at once.
        """
        if self._closed:
            return
        self._closed = True
        self.stop_heartbeat()
        self.unregister()
        self.connector.delete_heartbeat(self.worker_id)

    def __enter__(self) -> Worker:
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    def __del__(self):
        # Best effort: during interpreter shutdown the connector may be
        # partially torn down. Prefer close() or a `with` block.
        try:
            self.close()
        except Exception:
            pass

    def get_reply_to(self, request: dict) -> str | None:
        """Returns the queue name where response is going to be sent.

        Returns the content of the key "reply_to" in request, if it is
        not None. If it is None, or the key "reply_to" does not exists,
        returns the content of the attribute "reply_to_default"
        of the Worker instance.

        If the above fails, generates the queue name from the key "id"
        of request, assuming format "{client_id}:num".

        Otherwise None.

        Note: The JSON RPC 2.0 specification does not have a "reply_to" attribute
        in the message object. If we want to be compliant we should able to respond
        to requests that have not defined "reply_to".

        The specification also says that "If there was an error in detecting the id
        in the Request object (e.g. Parse error/Invalid Request), it MUST be Null."

        Args:
            request (dict)

        Returns:
            str: The queue name to send the response or None.

        """
        if "reply_to" in request and request["reply_to"] is not None:
            return request["reply_to"]

        if self.reply_to_default is not None:
            return self.reply_to_default

        if "id" in request:
            try:
                return self.connector.get_reply_to_from_id(request["id"])
            except Exception:
                return None

        return None

    def enhance_response(
        self,
        response: dict,
        request: dict,
        dispatched_to: str | None = None,
        execution_start: float | None = None,
    ) -> None:
        """Annotates a response dict with routing info and metadata.

        Adds the temporary keys `reply_to`, `is_notification` and `method`
        (used for routing and removed by `clean_response` before sending)
        and the `metadata` key (worker, queue, method and timing) that is
        sent with the response.

        Args:
            response (dict): Response dict to annotate (modified in place).
            request (dict): Request being answered.
            dispatched_to (str, optional): Queue where the request was
                received. Defaults to None.
            execution_start (float, optional): time() when the method
                execution started. Defaults to None.

        """
        r = response
        # To be cleaned
        r["reply_to"] = self.get_reply_to(request)
        r["is_notification"] = request["is_notification"]
        r["method"] = request["method"]
        # metadata
        r["metadata"] = {}
        if dispatched_to is not None:
            r["metadata"]["queue"] = dispatched_to
        r["metadata"]["worker"] = self.worker_id
        r["metadata"]["method"] = request["method"]
        r["metadata"]["timing"] = {}
        if "timing" in request and "request_sent" in request["timing"]:
            r["metadata"]["timing"]["request_sent"] = request["timing"]["request_sent"]
        if execution_start is not None:
            r["metadata"]["timing"]["execution_start"] = execution_start
        r["metadata"]["timing"]["execution_finish"] = time()

    def clean_response(self, response: dict) -> None:
        "Removes the temporary routing keys added by `enhance_response`."
        to_del = ["reply_to", "is_notification", "method"]
        for k in to_del:
            if k in response:
                del response[k]

    def error(
        self,
        code: int,
        request: dict,
        dispatched_to: str | None = None,
        execution_start: float | None = None,
    ) -> dict:
        "Builds an annotated error response for `request` (see `enhance_response`)."
        id_ = request.get("id", None)
        e = error_response(code, id=id_, with_trace=self.with_trace)
        self.enhance_response(e, request, dispatched_to, execution_start)
        return e

    def result(
        self,
        result: Any,
        request: dict,
        dispatched_to: str | None = None,
        execution_start: float | None = None,
    ) -> dict:
        "Builds an annotated result response for `request` (see `enhance_response`)."
        id_ = request.get("id", None)
        r = result_response(result, id=id_)
        self.enhance_response(r, request, dispatched_to, execution_start)
        return r

    def _exec_method_in_queue(
        self,
        queue_ref: str,
        method: str,
        args: list | None = None,
        kwargs: dict | None = None,
    ) -> Any:
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        funcs = self.requests_queues[queue_ref][0]
        return funcs[method](*args, **kwargs)

    def exec_method(
        self,
        method: str,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
    ) -> Any:
        """Executes a registered method locally (without messages).

        Args:
            method (str): Name of a method registered in `queue`.
            args (list, optional): Positional args. Defaults to None.
            kwargs (dict, optional): Named args. Defaults to None.
            queue (str, optional): Simple name of the queue that provides
                the method. Currently required.

        Returns:
            Result of the method call.

        Raises:
            ValueError: If `queue` is None.

        """
        if queue is not None:
            queue_ref = self.connector.get_requests_queue(queue)
            return self._exec_method_in_queue(queue_ref, method, args, kwargs)
        else:
            # TO DO: look up the queue that provides the method.
            raise ValueError("queue argument can't be None.")

    def process_single_request(self, request: dict, dispatched_to: str) -> dict | None:
        """Process a single request.

        Args:
            request (dict): Deserialized single request.
            dispatched_to (str): Queue where the request was received.
                The sole purpose of dispatched_to is to carry the value
                of the queue where the request was received for logging
                purposes. Not pretty.

        Returns:
            dict or None: result_response or error_response or None.
                If None, equivalent to error_response(-32600).

        """
        if not is_single_request(request):
            logger.error(
                f"{timestamp()} Couldn't be processed Individual Request from Batch Request from queue: {dispatched_to}"
            )
            return None  # error_response(-32600)

        is_notification = request["is_notification"]
        id_ = request.get("id", None)
        reply_to = self.get_reply_to(request)

        if request["method"] not in self.requests_queues[dispatched_to][0]:
            return self.error(-32601, request, dispatched_to)

        # send ack message
        if (
            not is_notification
            and reply_to is not None
            and "ack" in request
            and request["ack"]
        ):
            msg = ack(id_, self.worker_id, dispatched_to)
            self.connector.enqueue(reply_to, msg)
            logger.debug(
                f"{timestamp()} Worker: {self.worker_id} Sent Ack to queue: {reply_to} for request {id_}"
            )

        args = request["args"] if "args" in request else []
        kwargs = request["kwargs"] if "kwargs" in request else {}

        execution_start = time()

        # Invalid params (-32602) only when the arguments do not fit the
        # signature. A TypeError raised inside the function is an internal
        # error (-32603) like any other exception.
        try:
            check_params(
                self.requests_queues[dispatched_to][0][request["method"]], args, kwargs
            )
        except TypeError:
            return self.error(-32602, request, dispatched_to, execution_start)

        try:
            result = self._exec_method_in_queue(
                dispatched_to, request["method"], args, kwargs
            )
            return self.result(result, request, dispatched_to, execution_start)

        except Exception:
            return self.error(-32603, request, dispatched_to, execution_start)

    def process_request(
        self, request: dict | list, dispatched_to: str
    ) -> dict | list | None:
        """Process a request. Could be either a single or a batch request.

        Args:
            request (dict or list): Deserialized request. Either a single
                request (dict) or a batch request (list of dicts).
            dispatched_to (str): Queue where the request was received.
                The sole purpose of dispatched_to is to carry the value
                of the queue where the request was received for logging
                purposes. Not pretty.

        Returns:
            dict or list(dict) or None: result_response or error_response or list
                with both kind of responses. If None, equivalent to error_response(-32600).

        """
        if is_single_request(request):
            logger.debug(
                f"{timestamp()} Worker: {self.worker_id} Received Single Request for method: {request['method']} from queue: {dispatched_to}"
            )
            return self.process_single_request(request, dispatched_to)

        elif is_batch_request(request):
            # A Batch Request generate a Batch Response.
            # Should consider whether it makes sense to allow sending
            # individual responses as soon as they are available.
            logger.debug(
                f"{timestamp()} Worker: {self.worker_id} Received Batch Request with {len(request)} requests from queue: {dispatched_to}"
            )
            if len(request) == 0:
                return None  # error_response(-32600)

            responses = [
                self.process_single_request(rq, dispatched_to) for rq in request
            ]
            response = [r for r in responses if r is not None]

            if len(response) > 0:
                return response

            # Should not happen
            logger.error(
                f"{timestamp()} Worker: {self.worker_id} No responses where processed for Batch Request from queue: {dispatched_to}"
            )
            return None  # error_response(-32600)

        else:
            logger.error(
                f"{timestamp()} Worker: {self.worker_id} Received neither a Single nor a Batch Request from queue: {dispatched_to}"
            )
            return None  # error_response(-32600)

    def shuffled_queues(self) -> list:
        """Returns the queue refs sorted by priority (highest first).

        Queues with the same priority are shuffled on every call.
        """
        tuples = [
            (k, v[1]) for k, v in self.requests_queues.items()
        ]  # [(queue, priority), ...]
        grouped = {}
        for k, v in tuples:
            grouped.setdefault(v, []).append(k)

        sorted_grouped = dict(sorted(grouped.items(), reverse=True))
        for key in sorted_grouped:
            random.shuffle(sorted_grouped[key])

        return [value for _, values in sorted_grouped.items() for value in values]

    def run_once(self, timeout: float = -1) -> None:
        """Pops one request (single or batch) and processes it.

        Waits on all the worker's queues in priority order, processes the
        first available request and sends the response(s), unless they are
        notifications.

        Args:
            timeout (float): Maximum wait time in seconds. Defaults to -1
                (< 0 waits indefinitely).

        Raises:
            ValueError: If the worker has no queues to listen.

        """
        sorted_queues = self.shuffled_queues()

        if len(sorted_queues) == 0:
            raise ValueError("No queues to listen.")

        # `pop_multiple` returns tuple (queue name, request), or None, if timeout
        # returns only ONE request (can be a Batch Request)
        # queues are sorted by priority, which is something that is not
        # almost never desirable. Should mixed queues with high priority as
        # the worker's queue (with the 'info' method, with others that i should shuffle. Better done
        # at the worker module.
        request_with_priority = self.connector.pop_multiple(
            sorted_queues, timeout=timeout
        )

        if request_with_priority is not None:
            dispatched_to, request = request_with_priority

            # Process the request. Could be either a Single or a Batch request.
            processed = self.process_request(request, dispatched_to)

            if processed is None:  # error_response(-32600)
                return

            if is_single_response(processed):
                id_ = processed.get("id", None)
                reply_to = processed.get("reply_to", None)
                method = processed["method"]
                is_notification = (
                    processed["is_notification"] or id_ is None or reply_to is None
                )
                rtype = "RESULT" if "result" in processed else "ERROR"
                if not is_notification:
                    self.clean_response(processed)
                    self.connector.enqueue(reply_to, processed)
                    logger.debug(
                        f"{timestamp()} Worker: {self.worker_id} Sent Single {rtype} Response with id {id_} for method: {method} to queue: {reply_to}"
                    )
                else:
                    logger.debug(
                        f"{timestamp()} Worker: {self.worker_id} Processed Notification for method: {method} from queue: {dispatched_to}"
                    )

            elif is_batch_response(processed):
                # Generates one Batch Response for each queue in reply_to.
                # Each request within the Batch Request could have a different `reply_to`.
                # Could implement a parallel computation of Batch Requests in the future.
                batch = {}
                for one_single_response in processed:
                    id_ = one_single_response.get("id", None)
                    reply_to = one_single_response.get("reply_to", None)
                    method = one_single_response["method"]
                    is_notification = (
                        one_single_response["is_notification"]
                        or id_ is None
                        or reply_to is None
                    )
                    rtype = "RESULT" if "result" in one_single_response else "ERROR"
                    if not is_notification:
                        logger.debug(
                            f"{timestamp()} Worker: {self.worker_id} Appending {rtype} response with id: {id_} for method: {method} from Batch Request from queue: {dispatched_to}"
                        )
                        self.clean_response(one_single_response)
                        batch.setdefault(reply_to, []).append(one_single_response)
                    else:
                        logger.debug(
                            f"{timestamp()} Worker: {self.worker_id} Processed Notification for method: {method} from Batch Request from queue: {dispatched_to}"
                        )

                for reply_queue in batch:
                    self.connector.enqueue(reply_queue, batch[reply_queue])
                    logger.debug(
                        f"{timestamp()} Worker: {self.worker_id} Sent Batch Response with {len(batch[reply_queue])} items to queue {reply_queue}"
                    )
        else:
            logger.debug(
                f"{timestamp()} Worker: {self.worker_id} run_once method timeout"
            )

    def run(self, timeout: float | None = None) -> None:
        """Listen and process requests, forever or for `timeout` seconds.

        Any exception raised by the connector propagates and ends the loop.
        For a long-running service prefer `run_forever`. The heartbeat
        thread runs while the loop does (see `start_heartbeat`).
        """
        self.start_heartbeat()
        try:
            if timeout is None or timeout <= -0.00001:
                while True:
                    self.run_once(timeout=-1.0)

            t_0 = time()
            time_left = timeout
            while time_left > 0:
                self.run_once(timeout=time_left)
                time_left = timeout - (time() - t_0)
        finally:
            self.stop_heartbeat()

    def stop(self) -> None:
        """Asks `run_forever` to return after the current iteration.

        Can be called from a registered function (it runs inside the loop)
        or from another thread. A blocking pop is not interrupted: the loop
        exits once the connector returns.
        """
        self._stop_requested = True

    def run_forever(
        self,
        backoff: tuple = (1.0, 60.0),
        max_consecutive_errors: int | None = None,
    ) -> None:
        """Listen and process requests until `stop` is called, surviving errors.

        Unlike `run`, exceptions raised while waiting for or dispatching a
        request (connector failures such as a shared drive not responding,
        lock timeouts, corrupt messages...) are logged and the loop retries
        after an exponential backoff. Exceptions raised by the registered
        functions never reach this loop: they are answered as error
        responses. `KeyboardInterrupt` and `SystemExit` are not caught.
        The heartbeat thread runs while the loop does (see
        `start_heartbeat`).

        Args:
            backoff (tuple): (first, max) seconds to wait after an error.
                The wait doubles on every consecutive error, up to `max`,
                and resets after a successful iteration.
                Defaults to (1.0, 60.0).
            max_consecutive_errors (int, optional): If not None, re-raise
                the exception once this many errors happen in a row.
                Defaults to None (retry forever).

        Raises:
            ValueError: If the worker has no queues to listen.

        """
        if len(self.requests_queues) == 0:
            raise ValueError("No queues to listen.")

        first_wait, max_wait = backoff
        consecutive_errors = 0
        self._stop_requested = False

        self.start_heartbeat()
        try:
            while not self._stop_requested:
                try:
                    self.run_once(timeout=-1.0)
                    consecutive_errors = 0
                except Exception:
                    consecutive_errors += 1
                    wait = min(max_wait, first_wait * 2 ** (consecutive_errors - 1))
                    logger.exception(
                        f"{timestamp()} Worker: {self.worker_id} error #{consecutive_errors} in the main loop. Retrying in {wait:.1f}s"
                    )
                    if (
                        max_consecutive_errors is not None
                        and consecutive_errors >= max_consecutive_errors
                    ):
                        raise
                    sleep(wait)
        finally:
            self.stop_heartbeat()

        logger.info(f"{timestamp()} Worker: {self.worker_id} stopped.")
