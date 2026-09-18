"""Base class and behavioural contract for transports (connectors).

A connector gives `Client` and `Worker` four things inside a *namespace*:

- FIFO queues of Python objects (requests and responses),
- a small set store for the registry (which queues serve each method and
  which workers listen on each queue),
- two counters, for unique client and worker ids,
- a key/value store of shared variables (`set_variable`/`get_variable`).

`Connector` implements naming and the registry once, on top of a handful of
primitives that each transport provides. The class docstring states the
rules every implementation must follow; `tests/test_connector.py` checks
them against every connector.
"""

from __future__ import annotations

import logging
import random
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from contextlib import AbstractContextManager, nullcontext
from typing import Any

logger = logging.getLogger(__name__)

# Key families of the registry (set store):
#   {METHOD_QUEUES}{sep}{method}    -> {queue_ref, ...}  queues serving a method
#   {WORKERS_QUEUE}{sep}{queue_ref} -> {worker_id, ...}  workers listening on a queue
# Key family of the shared variables (value store):
#   {VARIABLES}{sep}{name}          -> value
METHOD_QUEUES = "method_queues"
WORKERS_QUEUE = "workers_queue"
VARIABLES = "variables"


class Connector(ABC):
    """Transport contract: FIFO queues, a set store, counters and variables in a namespace.

    Subclasses implement the primitives (the abstract methods) and inherit
    the naming scheme and the registry. `Client` and `Worker` only use the
    public methods.

    Rules every connector follows:

    Names
        Keys and queue references are built by `_key(*parts)`, which joins
        with `sep` and, if the transport needs it, prefixes a namespace.
        `requests_queue_name(get_requests_queue(q)) == q` for any simple
        name `q` without `sep`. Request ids are `{client_id}:{n}` (built by
        `Client`); a client id may itself contain ``:`` (Redis ids do), so
        `get_reply_to_from_id` splits on the last one and
        `get_reply_to_from_id(f"{cid}:{n}") == get_responses_queue(cid)`.
        Ids from `get_client_id` and `get_server_id` are unique for the life
        of the namespace: a counter that only `clean_namespace` resets.

    Queues
        `enqueue` appends; each queue is FIFO. `pop` and `pop_multiple`
        return `(queue_ref, obj)` or None on timeout: `timeout < 0` waits
        indefinitely, `timeout == 0` checks once without waiting and
        `timeout > 0` waits at most that many seconds. `pop_multiple`
        checks the queues in the given order (highest priority first) and
        returns the first message found. `pop_all` never blocks and returns
        `[obj, ...]` in FIFO order. A message the serializer cannot decode
        is logged at ERROR and skipped: it is never raised and does not
        extend the wait. A responses queue has a single consumer; a requests
        queue may have many, and each message reaches exactly one of them.

    Registry
        `register_methods` is additive and idempotent (sets).
        `unregister_methods(worker_id)` removes the worker from every queue;
        a queue left without workers is dropped and removed from every
        method, and a method left without queues is dropped. This is
        coarse-grained: a method stays available while any queue serving it
        still has a worker, even if that worker never offered the method.
        `methods_registry` and `workers_registry` return plain dicts of
        lists (snapshots). `random_queue_for_method` returns None when no
        queue serves the method.

    Variables
        `set_variable(name, value)` stores a Python object under `name`,
        shared by every client and worker of the namespace.
        `get_variable(name, default=None)` returns a *copy* of it (the
        value travels through the serializer, so mutating the returned
        object changes nothing shared), or `default` if the name is not
        set. `delete_variable(name)` returns whether the name existed and
        `variables()` lists the names. Each call is atomic on its own and
        the last write wins: no lock and no expiry.
        `update_variable(name, fn, default=None)` is the one atomic
        read-modify-write: it runs `fn(current)` and stores the result
        while holding `_variable_lock(key)`, a lock per variable that
        transports shared by several processes must provide (the default
        is a no-op). A variable updated with `update_variable` must not be
        written with `set_variable`, which bypasses the lock. Variables live
        in their own key family, apart from the registry, and are not
        covered by `_registry_lock()`.

    Atomicity
        Every public registry operation runs inside `_registry_lock()`.
        Transports whose set primitives are atomic by themselves (Redis)
        keep the default no-op lock; the others (a shared filesystem) return
        a real lock. `_incr` must be atomic on its own. Queue operations are
        not covered by the lock: the transport itself must deliver each
        message to exactly one consumer.

    Namespaces
        `clean_namespace` deletes queues, registry, counters and variables,
        so ids start again from 1.

    Encoding
        Queues and variables carry Python objects. The connector owns the
        encoding (its serializer); every client and worker of a namespace
        must use the same one.

    Attributes:
        sep (str): Separator used to build keys and queue references.
        id_prefix (str): Prefix of client and worker ids
            (`{id_prefix}_client`, `{id_prefix}_server`).

    """

    sep: str = "_"
    id_prefix: str = "connector"

    # ---- primitives every transport implements ------------------------------

    @abstractmethod
    def clean_namespace(self) -> None:
        "Deletes every queue, registry entry and counter of the namespace."

    @abstractmethod
    def _incr(self, key: str) -> int:
        """Atomically increments the counter `key` and returns its new value.

        The first call on a fresh namespace returns 1.
        """

    @abstractmethod
    def _set_add(self, key: str, members: Iterable[str]) -> None:
        "Adds `members` to the set `key`, creating the set if needed."

    @abstractmethod
    def _set_discard(self, key: str, members: Iterable[str]) -> int:
        "Removes `members` from the set `key`. Returns how many were present."

    @abstractmethod
    def _set_members(self, key: str) -> set[str]:
        "Returns the members of the set `key` (empty if it does not exist)."

    @abstractmethod
    def _set_keys(self, prefix: str) -> list[str]:
        "Returns the keys of every set whose name starts with `prefix`."

    @abstractmethod
    def _set_delete(self, key: str) -> None:
        "Deletes the set `key`. No error if it does not exist."

    @abstractmethod
    def _value_set(self, key: str, value: Any) -> None:
        "Stores the Python object `value` under `key`, replacing any previous one."

    @abstractmethod
    def _value_get(self, key: str) -> Any:
        "Returns the object stored under `key`. Raises KeyError if there is none."

    @abstractmethod
    def _value_delete(self, key: str) -> bool:
        "Deletes `key`. Returns True if it existed, False otherwise."

    @abstractmethod
    def _value_keys(self, prefix: str) -> list[str]:
        "Returns the keys of every stored value whose name starts with `prefix`."

    @abstractmethod
    def enqueue(self, queue: str, msg: Any) -> None:
        "Appends `msg` (a Python object) to the FIFO queue `queue`."

    @abstractmethod
    def pop(self, queue: str, timeout: float = -1) -> tuple | None:
        """Pops the first message of `queue`. Used by clients (single consumer).

        Args:
            queue (str): Queue reference.
            timeout (float): < 0 waits indefinitely, 0 checks once, > 0
                waits at most that many seconds. Defaults to -1.

        Returns:
            tuple: `(queue, obj)`, or None on timeout.

        """

    @abstractmethod
    def pop_multiple(self, queues: list, timeout: float = -1) -> tuple | None:
        """Pops the first message found in `queues`, checked in order. Used by workers.

        Args:
            queues (list): Queue references, highest priority first.
            timeout (float): < 0 waits indefinitely, 0 checks once, > 0
                waits at most that many seconds. Defaults to -1.

        Returns:
            tuple: `(queue, obj)`, or None on timeout.

        """

    @abstractmethod
    def pop_all(self, queue: str) -> list:
        "Returns every message available in `queue`, in FIFO order, without waiting."

    # ---- hooks with a default ------------------------------------------------

    def _key(self, *parts: str) -> str:
        "Builds a key or queue reference from its parts."
        return self.sep.join(parts)

    def _registry_lock(self) -> AbstractContextManager:
        "Context manager held during registry operations. No-op by default."
        return nullcontext()

    def _variable_lock(self, key: str) -> AbstractContextManager:
        """Context manager held by `update_variable` around the read-modify-write of `key`.

        No-op by default. A transport shared by several processes must
        return a real lock, one per variable (a lock directory on the
        filesystem, a Redis lock).
        """
        return nullcontext()

    # ---- names ---------------------------------------------------------------

    def get_requests_queue(self, queue_name: str) -> str:
        "Returns the requests queue reference for a simple queue name."
        return self._key("requests", queue_name)

    def requests_queue_name(self, queue_ref: str) -> str:
        "Returns the simple queue name for a requests queue reference."
        return queue_ref.removeprefix(self._key("requests") + self.sep)

    def get_responses_queue(self, client_id: str) -> str:
        "Returns the responses queue reference for a client id."
        return f"{client_id}{self.sep}responses"

    def get_reply_to_from_id(self, id_str: str) -> str:
        "Derives the responses queue from a request id `{client_id}:{n}`."
        return self.get_responses_queue(id_str.rsplit(":", 1)[0])

    def get_client_id(self) -> str:
        "Generates a new unique client id, `{id_prefix}_client{sep}{n}`."
        return self._new_id("client")

    def get_server_id(self) -> str:
        "Generates a new unique worker id, `{id_prefix}_server{sep}{n}`."
        return self._new_id("server")

    def _new_id(self, kind: str) -> str:
        n = self._incr(self._key(f"n{kind}s"))
        return f"{self._key(f'{self.id_prefix}_{kind}')}{self.sep}{n}"

    # ---- registry ------------------------------------------------------------

    def register_methods(self, requests_queues_dict: dict, worker_id: str) -> None:
        """Publishes the worker's methods and the queues where they are served.

        Args:
            requests_queues_dict (dict): `{queue_ref: {method_name: fn, ...}, ...}`.
            worker_id (str): Id of the worker publishing the methods.

        """
        methods: dict = {}
        for queue_ref, func_dict in requests_queues_dict.items():
            for method in func_dict:
                methods.setdefault(method, []).append(queue_ref)

        with self._registry_lock():
            for method, queues in methods.items():
                self._set_add(self._key(METHOD_QUEUES, method), queues)
                logger.info(
                    f"Method {method} published as available for queues: {', '.join(queues)}"
                )
            for queue_ref in requests_queues_dict:
                self._set_add(self._key(WORKERS_QUEUE, queue_ref), [worker_id])

    def unregister_methods(self, worker_id: str) -> None:
        """Removes a worker from the registry.

        Queues left without workers are dropped and removed from every
        method; methods left without queues are dropped.
        """
        with self._registry_lock():
            empty_queues = self._discard_and_prune(
                WORKERS_QUEUE, [worker_id], "Queue", "workers"
            )
            if empty_queues:
                self._discard_and_prune(METHOD_QUEUES, empty_queues, "Method", "queues")

    def _discard_and_prune(
        self, family: str, members: list, kind: str, listeners: str
    ) -> list:
        "Discards `members` from every set of `family`; deletes and returns the emptied ones."
        prefix = self._key(family) + self.sep
        emptied = []
        for key in self._set_keys(prefix):
            if self._set_discard(key, members) and not self._set_members(key):
                self._set_delete(key)
                name = key.removeprefix(prefix)
                emptied.append(name)
                logger.info(
                    f"{kind} {name} has no remaining public {listeners} listening. Unregistered."
                )
        return emptied

    def methods_registry(self) -> dict:
        "Returns `{method: [queue_ref, ...]}`. Used by clients."
        return self._family_snapshot(METHOD_QUEUES)

    def workers_registry(self) -> dict:
        "Returns `{queue_ref: [worker_id, ...]}`. Used by clients."
        return self._family_snapshot(WORKERS_QUEUE)

    def _family_snapshot(self, family: str) -> dict:
        prefix = self._key(family) + self.sep
        with self._registry_lock():
            return {
                key.removeprefix(prefix): list(self._set_members(key))
                for key in self._set_keys(prefix)
            }

    def all_queues_for_method(self, method: str) -> list:
        "Returns every queue ref where requests for `method` can be sent."
        return list(self._set_members(self._key(METHOD_QUEUES, method)))

    def random_queue_for_method(self, method: str) -> str | None:
        "Returns a random queue ref serving `method`, or None if there is none."
        available = self.all_queues_for_method(method)
        return random.choice(available) if available else None

    # ---- variables -----------------------------------------------------------

    def set_variable(self, name: str, value: Any) -> None:
        """Stores `value` under `name`, shared by the whole namespace.

        The value goes through the connector's serializer, so it must be
        encodable by it (JSON on Redis by default). The last write wins.

        Args:
            name (str): Variable name.
            value: Python object to share.

        """
        self._value_set(self._key(VARIABLES, name), value)

    def get_variable(self, name: str, default: Any = None) -> Any:
        """Returns a copy of the shared variable `name`, or `default` if not set.

        Mutating the returned object does not change the shared value: call
        `set_variable` again to publish a change.

        Args:
            name (str): Variable name.
            default: Value returned when the variable is not set.
                Defaults to None.

        """
        try:
            return self._value_get(self._key(VARIABLES, name))
        except KeyError:
            return default

    def update_variable(self, name: str, fn: Callable, default: Any = None) -> Any:
        """Atomically replaces the shared variable `name` with `fn(current)`.

        The read, the call and the write happen while holding the lock of
        that variable, so concurrent updates from several processes are
        serialized and none is lost. Keep `fn` pure and quick: it runs with
        the lock held. Do not mix with `set_variable` on the same name, it
        does not take the lock.

        Args:
            name (str): Variable name.
            fn (callable): Receives the current value and returns the new one.
            default: Value passed to `fn` when the variable is not set.
                Defaults to None.

        Returns:
            The new value.

        Raises:
            Whatever `fn` raises; the variable is then left unchanged.
                A transport may also raise its lock error on timeout.

        """
        key = self._key(VARIABLES, name)
        with self._variable_lock(key):
            try:
                current = self._value_get(key)
            except KeyError:
                current = default
            new = fn(current)
            self._value_set(key, new)
        return new

    def delete_variable(self, name: str) -> bool:
        "Deletes the shared variable `name`. Returns True if it existed."
        return self._value_delete(self._key(VARIABLES, name))

    def variables(self) -> list[str]:
        "Returns the names of every shared variable, sorted."
        prefix = self._key(VARIABLES) + self.sep
        return sorted(key.removeprefix(prefix) for key in self._value_keys(prefix))
