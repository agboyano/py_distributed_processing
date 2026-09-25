from __future__ import annotations

import base64
import logging
import random
import time
from datetime import datetime
from typing import Any, Callable

import dill

from .async_result import AsyncResult
from .connector import MISSING, Connector
from .messages import is_ack, is_batch_response, is_single_response, single_request

logger = logging.getLogger(__name__)

# Valid values of `Client.check_registry` (see the property).
CHECK_REGISTRY_MODES = ("cache", "always", "never")


def timestamp() -> str:
    return datetime.now().isoformat()


# Implementation notes.
#
# The function is pickled with dill, not pickle: dill serializes lambdas,
# closures and functions defined in a notebook or in `__main__` by value,
# so they travel whole. Functions imported from a module are still pickled
# by reference, and the worker must be able to import that module.
#
# `recurse=True` is passed explicitly. With dill's default, a function
# defined in a notebook or in `__main__` travels without the globals it
# refers to (a constant, an imported class), and the worker raises
# `NameError` when it runs it. With `recurse=True` dill pickles the globals
# the function actually uses: values by value, imported modules and
# classes by reference. `utils` sets the same option in `dill.settings`,
# so before this the outcome depended on whether `utils` had been
# imported first.
#
# dill.dumps returns bytes, and the params of a request travel through the
# connector's serializer like any other message. The default serializer on
# Redis is JSON, which cannot carry bytes, and a future connector may use
# any text format. Encoding the pickle in base64 and decoding to an ASCII
# str makes the payload a plain string that every serializer accepts, at
# the cost of about one third more size. The encoding is unconditional, even
# when the connector could carry bytes (pickle, joblib), so the wire format
# of `eval_py_function` requests is the same on every transport and the
# worker only has to reverse one thing: `dill.loads(base64.b64decode(s))`.
#
# The result is the positional params list of the request, in the order
# `eval_py_function(str_fn, args, kwargs)` expects.
def serialize_python_call(
    fn: Callable, args: list | None = None, kwargs: dict | None = None
) -> list:
    """Serializes a python call with dill for `eval_py_function`.

    Args:
        fn (function): Function to be serialized.
        args (list, optional): Positional arguments for `fn`. Defaults to None.
        kwargs (dict, optional): Keyword arguments for `fn`. Defaults to None.

    Returns:
        list: [base64-encoded dill-serialized `fn`, args, kwargs], the
            positional args expected by the worker's `eval_py_function`.
            The globals that `fn` uses (constants, imported names) travel
            with it; modules and classes are referenced by name and must
            be importable on the worker.

    """
    args = [] if args is None else args
    kwargs = {} if kwargs is None else kwargs
    pickled_fn = dill.dumps(fn, recurse=True)
    return [base64.b64encode(pickled_fn).decode("ascii"), args, kwargs]


class Client:
    """Sends RPC requests over a connector and collects the responses.

    Requests are JSON-RPC 2.0 style messages sent to the requests queues
    served by workers. Responses are cached until they are consumed by
    `wait_one_response` (usually through the `get` method of an `AsyncResult`).

    Args:
        connector (Connector): Transport instance (e.g. `RedisConnector`,
            `FileSystemConnector`). Messages are handed to the connector as
            Python objects; the connector owns the wire encoding.
        client_id (str, optional): Client identifier. Defaults to None.
            If None, a new one is requested to the connector.
        check_registry (str): How to choose the queue of a request sent
            **without** an explicit `queue`. Defaults to 'cache'. An
            explicit `queue` is always used as is, in every mode: the
            registry is not consulted (so queues added with
            `register=False` are reachable) and the worker answers -32601
            for methods it does not offer.
            - 'cache': choose among the queues in the local registry cache
              (refreshed with `update_registry_cache`). ValueError if no
              queue serves the method.
            - 'always': check the connector registry on every request.
              Huge overhead. ValueError if no queue serves the method.
            - 'never': send to `default_queue` without consulting the
              registry.
            Case-insensitive, leading/trailing spaces ignored; any other
            value raises ValueError, at construction or on assignment.
            Setting 'cache' on a client created in another mode fills the
            registry cache.
        use_reply_to (bool): If True, requests include the client's
            responses queue in the `reply_to` key. Defaults to False
            (workers derive the responses queue from the request `id`).
        default_queue (str): Simple name of the default requests queue,
            used only with `check_registry='never'` and no explicit
            `queue`. Defaults to 'default'.
        timeout (float, optional): Default timeout in seconds for waiting
            responses. Defaults to 5 * 60. If None, wait forever.

    """

    def __init__(
        self,
        connector: Connector,
        client_id: str | None = None,
        check_registry: str = "cache",
        use_reply_to: bool = False,
        default_queue: str = "default",
        timeout: float | None = 5 * 60,
    ):
        self.connector = connector

        self.use_reply_to = use_reply_to

        # Cache for pending responses with id.
        # ids as keys and time() of message creation as values.
        self.pending: dict = {}
        # Cache for responses with id.
        # ids as keys and deserialized responses as values.
        self.responses: dict = {}
        # Cache for notifications (received messages with no id).
        # Deserialized responses as values.
        self.notifications: list = []
        # Cache for acks
        self.acks: dict = {}
        # Used responses (wait_one_response).
        self.responses_used: set = set()

        # Always has both keys, so lookups never raise KeyError. Must exist
        # before check_registry is set: the setter fills it for 'cache'.
        self._registry: dict = {"methods": {}, "workers": {}}
        self.check_registry = check_registry

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

    @property
    def check_registry(self) -> str:
        """Queue selection mode for requests sent without an explicit `queue`.

        One of `CHECK_REGISTRY_MODES`: 'cache', 'always' or 'never' (see the
        class docstring for what each one does). Assignment normalizes the
        value (case-insensitive, surrounding spaces ignored) and raises
        `ValueError` for anything else. Assigning 'cache' fills the registry
        cache if it is empty, so a client created in another mode can switch
        to 'cache' at runtime without calling `update_registry_cache`.

        Raises:
            ValueError: If the value is not one of the three modes.

        """
        return self._check_registry

    # Implementation notes.
    #
    # The mode used to be a plain attribute compared literally against
    # "cache" and "always": anything else, including a typo ("chache") or
    # another case ("Never", as in the filesystem example notebooks), was
    # silently treated as "never". Normalizing keeps the notebooks working;
    # raising on unknown values makes the typo visible at once. Filling the
    # cache on "cache" removes a KeyError that hit a client created with
    # "never" and switched to "cache" later, whose cache had never been read.
    @check_registry.setter
    def check_registry(self, value: str) -> None:
        mode = value.strip().lower() if isinstance(value, str) else None
        if mode not in CHECK_REGISTRY_MODES:
            raise ValueError(
                f"check_registry must be one of {CHECK_REGISTRY_MODES}, got {value!r}."
            )
        self._check_registry = mode
        if mode == "cache" and not self._registry["methods"]:
            self.update_registry_cache()

    def set_default_queue(self, queue: str) -> None:
        "Sets the default requests queue (used only with `check_registry='never'` and no explicit `queue`)."
        self.default_queue_ref = self.connector.get_requests_queue(queue)

    # ---- shared variables (delegated to the connector) -----------------------

    def set_variable(self, name: str, value: Any) -> None:
        """Stores a variable shared by every client and worker of the namespace.

        Same as `connector.set_variable`. The value goes through the
        connector's serializer (JSON on Redis by default) and the last write
        wins. Typical use: publish a parameter once instead of sending it
        with every request.

        Args:
            name (str): Variable name.
            value: Python object to share.

        Example:
            client.set_variable("valuation_date", "2026-09-18")
            client.rpc_sync("price", [isin])  # the worker reads the date

        """
        self.connector.set_variable(name, value)

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Returns a copy of a shared variable, or `default` if it is not set.

        Same as `connector.get_variable`. Mutating the returned object does
        not change the shared value.
        """
        return self.connector.get_variable(name, default)

    def update_variable(self, name: str, fn: Callable, default: Any = MISSING) -> Any:
        """Atomically replaces a shared variable with `fn(current)`; returns the new value.

        Same as `connector.update_variable`: the read-modify-write runs
        under a per-variable lock, so concurrent updates are not lost.
        Keep `fn` pure and quick. A missing variable raises `KeyError`
        unless `default` is given, e.g.
        `client.update_variable("done", lambda n: n + 1, default=0)`.
        """
        return self.connector.update_variable(name, fn, default)

    def delete_variable(self, name: str) -> bool:
        "Deletes a shared variable. Returns True if it existed."
        return self.connector.delete_variable(name)

    def variables(self) -> list:
        "Returns the sorted names of the shared variables."
        return self.connector.variables()

    def to_requests_queue_ref(self, queue_name: str) -> str:
        "Returns the connector queue reference for a simple queue name."
        return self.connector.get_requests_queue(queue_name)

    def simple_queue_name(self, queue_ref: str) -> str:
        "Returns the simple queue name for a connector queue reference."
        return self.connector.requests_queue_name(queue_ref)

    def generate_id(self) -> str:
        "Generates a new request id with format {client_id}:{n}."
        self.last_request_idnumber += 1
        self.last_request_id = f"{self.client_id}:{str(self.last_request_idnumber)}"
        return self.last_request_id

    # Implementation notes.
    #
    # A refresh prunes first. It is safe for two reasons. The worker
    # publishes its heartbeat interval, so `Connector.dead_workers` gives
    # each worker its own tolerance and a healthy slow worker is not pruned
    # by mistake. And `gather` decides "lost" from the answers of the alive
    # workers, not from dead entries in the registry, so a prune at any
    # moment cannot hide a loss. It is also reversible for a live worker:
    # one that could not beat for two intervals publishes its queues and
    # methods again at its next beat (see `Worker.start_heartbeat`). The
    # prune uses the transport's default `max_age`, not a caller's:
    # pruning is a shared write and the threshold belongs to the
    # transport. `prune_dead_workers(max_age)` is the way to use another
    # one.
    def update_registry_cache(self, prune: bool = True) -> None:
        """Refreshes the local cache of the methods and workers registries.

        Args:
            prune (bool): If True, first unregister the workers whose
                heartbeat is too old for the connector's default `max_age`
                (`Connector.prune_dead_workers()`), so the cache does not
                list dead queues. Defaults to True.

        """
        if prune:
            self.connector.prune_dead_workers()
        self._registry["methods"] = self.connector.methods_registry()
        self._registry["workers"] = self.connector.workers_registry()

    def registry(self, update: bool = False) -> dict:
        """Returns the cached registry with simple queue names.

        Args:
            update (bool): If True, refresh the cache first with
                `update_registry_cache`, which also prunes the dead
                workers. Defaults to False.

        Returns:
            dict: {"methods": {method: [queue_name, ...]},
                "workers": {queue_name: [worker_id, ...]}}.

        """
        if update:
            self.update_registry_cache()

        return {
            "methods": {
                method: [self.simple_queue_name(x) for x in queue_refs]
                for method, queue_refs in self._registry["methods"].items()
            },
            "workers": {
                self.simple_queue_name(queue_ref): workers
                for queue_ref, workers in self._registry["workers"].items()
            },
        }

    def _all_queue_refs_for_method(self, method: str) -> list:
        if self.check_registry == "always":
            return self.connector.all_queues_for_method(method)
        elif self.check_registry == "cache":
            if method not in self._registry["methods"]:
                # Stale cache? Refresh once, as _select_queue_ref does.
                self.update_registry_cache()
            return self._registry["methods"].get(method, [])
        else:
            return [self.default_queue_ref]

    def all_queues_for_method(self, method: str, update: bool = False) -> list:
        """Returns the simple names of the queues that serve a method.

        Args:
            method (str): Remote function name.
            update (bool): If True and `check_registry` is 'cache', refresh
                the cache first. Defaults to False.

        Returns:
            list: List of simple queue names.

        """
        if update and self.check_registry == "cache":
            self.update_registry_cache()
        return [
            self.simple_queue_name(x) for x in self._all_queue_refs_for_method(method)
        ]

    def all_workers_for_method(self, method: str, update: bool = False) -> list:
        """Returns the ids of the workers that serve a method.

        Args:
            method (str): Remote function name.
            update (bool): If True, refresh the cache first. Defaults to False.

        Returns:
            list: Sorted list of worker ids.

        """
        if update or self.check_registry == "always":
            self.update_registry_cache()
        r = self._registry
        queues = r["methods"].get(method, [])
        ws = set()
        for q in queues:
            ws = ws.union(set(r["workers"].get(q, [])))

        return sorted(ws)

    # ---- heartbeats ----------------------------------------------------------

    # Implementation notes.
    #
    # A registered worker without a heartbeat key counts as alive. Workers
    # created with `heartbeat_interval=None`, driven with `run_once` only,
    # or running an older version never write one, and reporting them dead
    # would make every existing deployment look empty. A dead worker always
    # has a key: it wrote heartbeats while alive and never deleted them.
    # The rule for "too old" lives in `Connector.dead_workers`, so this
    # method, `prune_dead_workers` and `gather` agree.
    def alive_workers(self, max_age: float | None = None, update: bool = True) -> dict:
        """Returns the registered workers that look alive, by queue.

        A worker is alive if its last heartbeat is not too old (see
        `Connector.dead_workers`: `max_age`, or three times its own
        interval if that is longer), or if it has no heartbeat at all
        (heartbeats disabled or not started). Queues whose workers are all
        dead are returned with an empty list.

        Args:
            max_age (float, optional): Seconds. The floor of the tolerance.
                Defaults to None: the connector's
                `default_heartbeat_max_age` (30 s; 61 s on the filesystem).
            update (bool): If True, refresh the registry cache first,
                which also prunes the workers that are dead for the
                connector's default `max_age`. Defaults to True.

        Returns:
            dict: `{queue_name: [worker_id, ...]}` with simple queue names
                and sorted ids.

        """
        if update:
            self.update_registry_cache()
        dead = self.connector.dead_workers(max_age)

        return {
            self.simple_queue_name(queue_ref): sorted(
                w for w in workers if w not in dead
            )
            for queue_ref, workers in self._registry["workers"].items()
        }

    def prune_dead_workers(self, max_age: float | None = None) -> list:
        """Unregisters the workers whose heartbeat is too old.

        Same as `connector.prune_dead_workers`, and refreshes the registry
        cache afterwards. Every cache refresh (`update_registry_cache`,
        `registry(update=True)`, `alive_workers(update=True)`, a cache
        miss in 'cache' mode, each `gather` step) does the same with the
        connector's default `max_age`; call this one for another
        threshold. Queue selection in 'always' mode reads the connector
        directly and does not prune. Workers without a heartbeat key are
        never pruned. A live worker pruned while it could not beat comes
        back by itself at its next heartbeat.

        Args:
            max_age (float, optional): Seconds. The floor of the tolerance,
                see `Connector.dead_workers`. Defaults to None: the
                connector's `default_heartbeat_max_age`.

        Returns:
            list: Sorted ids of the pruned workers.

        """
        dead = self.connector.prune_dead_workers(max_age)
        self.update_registry_cache(prune=False)
        return dead

    def _select_queue_ref(self, method: str) -> str:
        """Chooses the queue for a request sent without an explicit `queue`.

        Only called when no queue was given (an explicit queue is always used
        as is). The choice depends on `check_registry`:

        - 'always': a random queue serving `method`, read from the connector
          registry on every call.
        - 'cache': a random queue serving `method`, from the local cache;
          the cache is refreshed once if the method is missing.
        - 'never' (or any other value): `default_queue_ref`, without
          consulting the registry.

        Args:
            method (str): Remote method name.

        Returns:
            str: Queue reference.

        Raises:
            ValueError: With 'always'/'cache', if no queue serves `method`.
                `default_queue` is not used as a fallback in those modes.

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
        method: str,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
        id: str | None = None,
        reply_to: str | None = None,
        ack: bool | None = None,
        is_notification: bool = False,
        **options: Any,
    ) -> tuple:
        """Sends a single RPC `request`.

        If no `id` is provided and `is_notification` is False, generates a new one.
        Reusing an `id` constitutes a retry; the first response that is available
        will be used (with no guarantees).

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
                message, the worker can respond guessing the `response_queue` from the `request` `id`.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If given, the request is sent to that queue as is (the registry is
                not consulted; the worker answers -32601 if it lacks the method).
                If None, chosen by `_select_queue_ref` according to `check_registry`.
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

        self.connector.enqueue(queue_ref, sr)
        logger.debug(
            f"{timestamp()} Client: {self.client_id} sent request with id: {id_} to queue: {queue_ref}"
        )

        # Notifications have no id and expect no response: nothing to track.
        if id_ is not None:
            self.pending[id_] = time.time()
        return id_, self.simple_queue_name(queue_ref)

    def send_batch_request(
        self,
        requests_lst: list,
        queue: str | None = None,
        retry: bool | None = None,
        ack: bool | None = None,
        **options: Any,
    ) -> list:
        """Sends a batch request that will be executed by a single worker.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs), ...].
                The tuples match the first three positional args of the
                `single_request` function and must have exactly three items.
            queue (str, optional): Queue to send the batch request to. Defaults to None.
                If given, the batch is sent to that queue as is (the registry is not
                consulted; the worker answers -32601 for methods it does not offer).
                If None, chosen by `check_registry`: with 'cache'/'always', a random
                queue among those serving every method in `requests_lst` (ValueError
                if there is none); with 'never', `default_queue`.
            retry (bool, optional): Currently ignored. Batch requests carry no
                retry info, so they cannot be retried individually.
            ack (bool, optional): Currently ignored. The individual requests
                are sent without the `ack` key, so no ack messages are sent.
            **options: Additional arguments added to the each individual request under the 'options' key.

        Returns:
            list(str): List of ids of the individual sent requests.

        Raises:
            ValueError: If the batch is empty, or if `queue` is None and no queue
                serves every method in the batch.

        """
        if len(requests_lst) == 0:
            raise ValueError("Empty batch request.")

        if queue is not None:
            # Same rule as send_single_request: an explicit queue is used as
            # is. The registry only drives the choice when no queue is given;
            # once the caller names one, the worker's -32601 answers are the
            # check (and queues added with register=False stay reachable).
            queue_ref = self.connector.get_requests_queue(queue)
        else:
            queue_refs_sets = [
                set(self._all_queue_refs_for_method(x[0])) for x in requests_lst
            ]

            # The batch is processed by a single worker, so the target queue
            # must be available for every method in the batch. With
            # check_registry 'never' every set is {default_queue}.
            requests_queue_refs = list(set.intersection(*queue_refs_sets))

            if len(requests_queue_refs) == 0:
                raise ValueError("No common queue for batch request.")

            queue_ref = random.choice(requests_queue_refs)

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

        self.connector.enqueue(queue_ref, batch_request)

        ids = [t["id"] for t in batch_request]
        logger.debug(
            f"{timestamp()} Client: {self.client_id} sent batch request with {len(ids)} requests to queue: {queue_ref}"
        )

        for id in ids:
            self.pending[id] = time.time()

        return ids

    def _responses_to_dicts(self, responses: list) -> tuple:
        """Classifies the responses received from the connector.

        Args:
            responses (list): List of response messages (Python objects, as
                returned by the connector's pop or pop_all).

        Returns:
            tuple [dict, list, dict]: (results_dict, no_id, acks_dict)

            results_dict (dict): Dictionary with the ids of the request as keys
                and the response as value. The response is a dict with either
                the key "result" or "error". The get method of the AsyncResult
                instance, associated with the id, returns the "result", if
                available, or throws an exception with the information in "error".
            no_id (list): List with all the responses that have no id (notifications).
            acks_dict (dict):  Dictionary with the ids of the request as keys
                and the ACKS as value.

        """
        results_dict = {}
        acks_dict = {}
        no_id = []

        for r in responses:
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
                    no_id.append(r)
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

        return results_dict, no_id, acks_dict

    def _update_responses_cache(self, responses: list) -> None:
        """Classifies responses and updates the caches.

        Updates the client caches responses, notifications, acks and pending.

        Args:
            responses (list): List of response messages, usually from pop or pop_all.
        """
        responses_dict, no_id, acks_dict = self._responses_to_dicts(responses)
        self.responses.update(responses_dict)
        self.notifications.extend(no_id)
        self.acks.update(acks_dict)
        pending = [k for k in self.pending.keys()]
        for id in pending:
            if id in self.responses:
                del self.pending[id]

        acks = [k for k in self.acks.keys()]
        for id in acks:
            if id in self.responses:
                del self.acks[id]

    def _update_cache_with_all_available_responses(self) -> None:
        all_responses = self.connector.pop_all(self.responses_queue)
        self._update_responses_cache(all_responses)

    def wait_responses(
        self, ids: list | None = None, timeout: float | None = None
    ) -> list:
        """Wait for the responses to the given request ids.

        Args:
            ids (list[str], optional): Request ids to wait for, as returned by
                `AsyncResult.id` — not `AsyncResult` instances themselves.
                Defaults to None, which waits for every pending id.
            timeout (float, optional): Defaults to None (self.timeout). If 0,
                the queue is checked once.

        Returns:
            list[str]: The ids still pending if the timeout expired, [] if all
                responses arrived.

        Raises:
            ValueError: If any id is neither in responses nor in pending.
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

        # timeout may still be None here if self.timeout is None:
        # in that case wait forever.
        forever = timeout is None
        t_0 = time.time()
        time_left = -1.0 if timeout is None else timeout

        while (len(pending) > 0) and (forever or time_left > 0.000001):
            next_resp = self.connector.pop(self.responses_queue, timeout=time_left)

            if (
                next_resp is not None
            ):  # if None then timeout, if not (queue_name, value)
                self._update_responses_cache([next_resp[1]])

            if timeout is not None:
                time_left = timeout - (time.time() - t_0)

            pending = [k for k in pending if k not in self.responses]

        return pending

    def wait_one_response(
        self, id: str, timeout: float | None = None, clean: bool = True
    ) -> dict:
        """Wait for the response with id=id.

        Used by the get method of AsyncResult.

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

    def clean_used(self) -> None:
        """Clean all responses that have been used at least once."""
        responses = [k for k in self.responses]
        for id in responses:
            if id in self.responses_used:
                del self.responses[id]

    def rpc_async(
        self,
        method: str,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
        retry: bool = False,
        ack: bool | None = None,
    ) -> AsyncResult:
        """Sends an asynchronous single request.

        Args:
            method (str): Remote function name.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If given, the request is sent to that queue as is (the registry is
                not consulted; the worker answers -32601 if it lacks the method).
                If None, chosen by `check_registry`: a queue serving the method in
                'cache'/'always' (ValueError if there is none), `default_queue` in 'never'.
            retry (bool): Include requests info in AsyncResult object in
                order to make possible retrying the request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            AsyncResult
        """
        request = (method, args, kwargs) if retry else None
        id, queue = self.send_single_request(method, args, kwargs, queue=queue, ack=ack)
        return AsyncResult(self, id, request, queue)

    def rpc_sync(
        self,
        method: str,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Sends a synchronous single request and waits for the result.

        Args:
            method (str): Remote function name.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If given, the request is sent to that queue as is (the registry is
                not consulted; the worker answers -32601 if it lacks the method).
                If None, chosen by `check_registry`: a queue serving the method in
                'cache'/'always' (ValueError if there is none), `default_queue` in 'never'.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        return self.rpc_async(method, args, kwargs, queue).get(timeout)

    def rpc_batch_async(
        self,
        requests_lst: list,
        queue: str | None = None,
        retry: bool = False,
        ack: bool | None = None,
    ) -> list:
        """Sends an asynchronous batch request that will be executed by a single worker.

        Each individual request within the batch has its own id assigned.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs), ...].
                The tuples must have exactly three items. A per-request queue is
                not supported: the whole batch is sent to a single common queue.
            queue (str, optional): Queue to send the batch request to. Defaults to None.
                If given, the batch is sent to that queue as is (the registry is not
                consulted; the worker answers -32601 for methods it does not offer).
                If None, chosen by `check_registry`: with 'cache'/'always', a random
                queue among those serving every method in `requests_lst` (ValueError
                if there is none); with 'never', `default_queue`.
            retry (bool, optional): Currently ignored. The AsyncResult objects
                are created without retry info, so the individual requests
                cannot be retried.
            ack (bool, optional): Currently ignored. The individual requests
                are sent without the `ack` key, so no ack messages are sent.

        Returns:
            list: List of AsyncResult objects of the individual requests within the batch.

        """
        ids = self.send_batch_request(requests_lst, queue=queue, retry=retry, ack=ack)
        return [AsyncResult(self, id) for id in ids]

    def rpc_batch_sync(
        self,
        requests_lst: list,
        timeout: float | None = None,
        queue: str | None = None,
    ) -> list:
        """Sends a synchronous batch request that will be executed by a single worker.

        Waits for the results with `safe_get`: a remote error or a timeout
        on an individual request gives None for that request.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs), ...]
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            queue (str, optional): Queue to send the batch request to.
                Defaults to None. If given, used as is; if None, chosen by
                `check_registry`. Same rules as `rpc_batch_async`.

        Returns:
            list: List of (results or None on error or timeout)

        Raises:
            ValueError: See `send_batch_request`.

        """
        fs = self.rpc_batch_async(requests_lst, queue=queue)
        return [f.safe_get(timeout=timeout) for f in fs]

    def rpc_multi_async(
        self, requests_lst: list, retry: bool = False, ack: bool | None = None
    ) -> list:
        """Sends multiple asynchronous requests that will be distributed among workers.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs, queue), ...].
                The tuples match the first four positional args of the `rpc_async`
                method. They can have less than four items. In this case, they will
                use the default values for the `rpc_async` args that are not in the tuple.
                A `queue` in the tuple is used as is; without it the queue is chosen
                per request according to `check_registry` (see `rpc_async`).
            retry (bool): Include requests info in the AsyncResult objects in
                order to make possible retrying every individual request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            list: List of AsyncResult objects.

        """
        return [self.rpc_async(*t[:], retry=retry, ack=ack) for t in requests_lst]

    def rpc_multi_sync(self, requests_lst: list, timeout: float | None = None) -> list:
        """Sends multiple synchronous requests that will be distributed among workers.

        Waits for the results.
        Uses safe_get, if there's an error in a function, returns None.

        Args:
            requests_lst (list): List of tuples [(method, args, kwargs, queue), ...].
                The tuples match the first four positional args of the `rpc_async`
                method. They can have less than four items. In this case, they will
                use the default values for the `rpc_async` args that are left out of the tuple.
                A `queue` in the tuple is used as is; without it the queue is chosen
                per request according to `check_registry` (see `rpc_async`).
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            list: List of (results or None on error or timeout, via `safe_get`).

        Raises:
            ValueError: If a request has no `queue` and no queue serves its method
                ('cache'/'always').

        """
        fs = self.rpc_multi_async(requests_lst, retry=False)
        return [f.safe_get(timeout=timeout) for f in fs]

    def rpc_async_fn(
        self,
        fn: Callable,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
        retry: bool = False,
        ack: bool | None = None,
    ) -> AsyncResult:
        """Sends an asynchronous single request with a local python function.

        Args:
            fn (function): Local function to be serialized and sent.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If given, the request is sent to that queue as is (the registry is
                not consulted; the worker answers -32601 if it lacks the method).
                If None, chosen by `check_registry`: a queue serving the method in
                'cache'/'always' (ValueError if there is none), `default_queue` in 'never'.
            retry (bool): Include requests info in AsyncResult object in
                order to make possible retrying the request. Defaults to False.
            ack (bool, optional): True if the worker sends a ack message when the request is received. False or
                None otherwise. Defaults to None.

        Returns:
            AsyncResult

        """
        py_call = serialize_python_call(fn, args=args, kwargs=kwargs)
        method, args = "eval_py_function", py_call
        request = (method, args, None) if retry else None
        id, queue = self.send_single_request(method, args=args, queue=queue, ack=ack)
        return AsyncResult(self, id, request, queue)

    def rpc_sync_fn(
        self,
        fn: Callable,
        args: list | None = None,
        kwargs: dict | None = None,
        queue: str | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Sends a synchronous single request with a local python function.

        Args:
            fn (function): Local function to be serialized and sent.
            args (list): Positional args. Defaults to [].
            kwargs (dict): Named args. Defaults to {}.
            queue (str, optional): Queue to send the request to. Defaults to None.
                If given, the request is sent to that queue as is (the registry is
                not consulted; the worker answers -32601 if it lacks the method).
                If None, chosen by `check_registry`: a queue serving the method in
                'cache'/'always' (ValueError if there is none), `default_queue` in 'never'.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.

        Returns:
            result

        Raises:
            TimeoutError
            RemoteException

        """
        return self.rpc_async_fn(fn, args, kwargs, queue).get(timeout)
