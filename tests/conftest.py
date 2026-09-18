import time
from collections import deque

import pytest

from distributed_processing.client import Client
from distributed_processing.connector import Connector
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker


class MemoryConnector(Connector):
    """In-memory connector: the smallest complete implementation of `Connector`.

    Client and worker share the same instance (same process), which makes
    it possible to unit test the whole request/response cycle without
    external infrastructure (redis, filesystem).

    Queues carry Python objects; the connector owns the encoding. Messages
    are stored encoded (JSON by default), like a real connector would, so
    the tests still check that every message is JSON-serializable.
    """

    sep = "_"
    id_prefix = "mem"

    def __init__(self, serializer=None):
        self.serializer = JsonSerializer() if serializer is None else serializer
        self.queues = {}  # queue_ref -> deque of encoded messages
        self.sets = {}  # registry key -> set of members
        self.counters = {}  # counter key -> int
        self.values = {}  # variable key -> encoded value

    def clean_namespace(self):
        self.queues.clear()
        self.sets.clear()
        self.counters.clear()
        self.values.clear()

    # --- primitives ---
    def _incr(self, key):
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    def _set_add(self, key, members):
        self.sets.setdefault(key, set()).update(members)

    def _set_discard(self, key, members):
        current = self.sets.get(key, set())
        members = set(members)
        removed = len(current & members)
        current -= members
        return removed

    def _set_members(self, key):
        return set(self.sets.get(key, set()))

    def _set_keys(self, prefix):
        return [k for k in self.sets if k.startswith(prefix)]

    def _set_delete(self, key):
        self.sets.pop(key, None)

    # --- variables ---
    def _value_set(self, key, value):
        self.values[key] = self.serializer.dumps(value)

    def _value_get(self, key):
        return self.serializer.loads(self.values[key])

    def _value_delete(self, key):
        return self.values.pop(key, None) is not None

    def _value_keys(self, prefix):
        return [k for k in self.values if k.startswith(prefix)]

    # --- queues ---
    def enqueue(self, queue, msg):
        self.queues.setdefault(queue, deque()).append(self.serializer.dumps(msg))

    def pop(self, queue, timeout=-1):
        q = self.queues.get(queue)
        if q:
            try:
                return (queue, self.serializer.loads(q.popleft()))
            except IndexError:
                pass
        # Non blocking (returns None as if it were a timeout). The client
        # loops while its own time_left > 0, so tests still work; the small
        # sleep avoids busy-spinning when a worker thread runs concurrently.
        time.sleep(0.005)
        return None

    def pop_multiple(self, queues, timeout=-1):
        deadline = time.time() + (
            timeout if timeout is not None and timeout > 0 else 0.05
        )
        while True:
            for name in queues:
                q = self.queues.get(name)
                if q:
                    try:
                        return (name, self.serializer.loads(q.popleft()))
                    except IndexError:
                        continue
            if time.time() >= deadline:
                return None
            time.sleep(0.005)

    def pop_all(self, queue):
        q = self.queues.get(queue, deque())
        out = []
        while q:
            try:
                out.append(self.serializer.loads(q.popleft()))
            except IndexError:
                break
        return out


def add(a, b):
    return a + b


def boom():
    raise RuntimeError("remote failure")


@pytest.fixture
def connector():
    return MemoryConnector()


@pytest.fixture
def worker(connector):
    w = Worker(connector)
    w.add_requests_queue("q", {"add": add, "boom": boom})
    w.update_methods_registry()
    return w


@pytest.fixture
def client(connector, worker):
    # Created after the worker so the registry cache is already populated.
    return Client(connector, check_registry="cache")
