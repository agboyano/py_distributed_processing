import time
from collections import deque

import pytest

from distributed_processing.client import Client
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker


class MemoryConnector:
    """In-memory connector implementing the connector contract.

    Client and worker share the same instance (same process), which makes
    it possible to unit test the whole request/response cycle without
    external infrastructure (redis, filesystem).
    """

    def __init__(self):
        self.queues = {}
        self.methods = {}  # method -> [queue_ref, ...]
        self.workers = {}  # queue_ref -> [worker_id, ...]
        self.nclients = 0
        self.nservers = 0

    # --- names ---
    def get_requests_queue(self, queue_name):
        return f"requests_{queue_name}"

    def requests_queue_name(self, queue_ref):
        return queue_ref.removeprefix("requests_")

    def get_responses_queue(self, client_id):
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str):
        return id_str.split(":")[0] + "_responses"

    def get_client_id(self):
        self.nclients += 1
        return f"mem_client_{self.nclients}"

    def get_server_id(self):
        self.nservers += 1
        return f"mem_server_{self.nservers}"

    # --- registry ---
    def register_methods(self, requests_queues_dict, worker_id):
        for queue_ref, func_dict in requests_queues_dict.items():
            for method in func_dict:
                self.methods.setdefault(method, [])
                if queue_ref not in self.methods[method]:
                    self.methods[method].append(queue_ref)
            self.workers.setdefault(queue_ref, [])
            if worker_id not in self.workers[queue_ref]:
                self.workers[queue_ref].append(worker_id)

    def unregister_methods(self, worker_id):
        for queue_ref in list(self.workers):
            if worker_id in self.workers[queue_ref]:
                self.workers[queue_ref].remove(worker_id)

    def methods_registry(self):
        return {k: list(v) for k, v in self.methods.items()}

    def workers_registry(self):
        return {k: list(v) for k, v in self.workers.items()}

    def all_queues_for_method(self, method):
        return list(self.methods.get(method, []))

    def random_queue_for_method(self, method):
        queues = self.all_queues_for_method(method)
        return queues[0] if queues else None

    # --- queues ---
    def enqueue(self, queue, msg):
        self.queues.setdefault(queue, deque()).append(msg)

    def pop(self, queue, timeout=-1):
        q = self.queues.get(queue)
        if q:
            try:
                return (queue, q.popleft())
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
                        return (name, q.popleft())
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
                out.append(q.popleft())
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
    w = Worker(JsonSerializer(), connector)
    w.add_requests_queue("q", {"add": add, "boom": boom})
    w.update_methods_registry()
    return w


@pytest.fixture
def client(connector, worker):
    # Created after the worker so the registry cache is already populated.
    return Client(JsonSerializer(), connector, check_registry="cache")
