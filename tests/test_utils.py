"""Tests of `distributed_processing.utils.node`.

The master runs in this process on a `MemoryConnector`. Each worker
subprocess builds its own `Worker` on its own `MemoryConnector`, so the
tests need no Redis nor shared directory: they exercise the subprocess
handshake and the bookkeeping. An RPC round trip through a spawned worker
needs a shared transport; see `test_filesystem_connector.py`.
"""

import time

import pytest
from conftest import MemoryConnector, add

from distributed_processing.utils import node
from distributed_processing.worker import Worker


def make_worker(queue="q"):
    "Worker constructor run inside the subprocess (pickled by reference)."
    w = Worker(MemoryConnector())
    w.add_requests_queue(queue, {"add": add})
    w.update_methods_registry()
    return w


def broken_worker():
    raise RuntimeError("constructor exploded")


class Spy:
    "Wraps a connector and records `unregister_methods` / `delete_heartbeat` calls."

    def __init__(self, connector):
        self._connector = connector
        self.unregistered = []
        self.deleted = []

    def unregister_methods(self, worker_id):
        self.unregistered.append(worker_id)
        return self._connector.unregister_methods(worker_id)

    def delete_heartbeat(self, worker_id):
        self.deleted.append(worker_id)
        return self._connector.delete_heartbeat(worker_id)

    def __getattr__(self, name):
        return getattr(self._connector, name)


NODE_METHODS = [
    "create_worker",
    "list_processes",
    "kill_process",
    "kill_processes",
    "kill_all_processes",
    "cleanup",
]


def call(master, method, *args):
    "Calls a node method locally, as an RPC request would."
    return master.exec_method(method, list(args), queue=master.worker_id)


@pytest.fixture
def master():
    m = Worker(Spy(MemoryConnector()), worker_id="node_test", heartbeat_interval=None)
    node(m, {"w": make_worker, "broken": broken_worker}, creation_processes_timeout=30)
    yield m
    call(m, "cleanup")


class TestBookkeeping:
    def test_methods_are_published_on_the_master_queue(self, master):
        queue_ref = master.connector.get_requests_queue("node_test")
        methods = master.connector.methods_registry()
        for name in NODE_METHODS:
            assert methods[name] == [queue_ref]
        assert call(master, "list_processes") == []
        assert call(master, "kill_process", 123456) is False
        assert call(master, "kill_processes", [1, 2]) == [False, False]
        assert call(master, "kill_all_processes") == []

    def test_unknown_worker_type_raises(self, master):
        with pytest.raises(ValueError, match="Unknown worker type"):
            call(master, "create_worker", "nope")


class TestSubprocesses:
    def test_create_list_and_kill(self, master):
        pid, worker_type, worker_id = call(master, "create_worker", "w", ["q2"])
        assert worker_type == "w"
        assert worker_id.startswith("mem_server")  # sent back by the child
        assert call(master, "list_processes") == [(pid, "w", worker_id)]

        assert call(master, "kill_process", pid) is True
        assert call(master, "list_processes") == []
        assert call(master, "kill_process", pid) is False
        # the master unregistered the killed worker and deleted its heartbeat
        assert master.connector.unregistered == [worker_id]
        assert master.connector.deleted == [worker_id]

    def test_failing_constructor_raises_with_the_remote_traceback(self, master):
        t_0 = time.time()
        with pytest.raises(RuntimeError, match="constructor exploded"):
            call(master, "create_worker", "broken")
        # detected as soon as the child reports, not after the timeout
        assert time.time() - t_0 < 25
        assert call(master, "list_processes") == []

    def test_closure_constructor_and_cleanup(self):
        # The notebook case: a constructor defined inside a function has no
        # importable name, dill pickles it by value.
        def make():
            return make_worker("q3")

        spy = Spy(MemoryConnector())
        m = Worker(spy, worker_id="node_closure", heartbeat_interval=1)
        node(m, {"local": make}, creation_processes_timeout=30)
        m.start_heartbeat()
        pid, _, worker_id = call(m, "create_worker", "local")
        assert "node_closure" in spy.heartbeats()

        assert call(m, "cleanup") == [pid]

        assert call(m, "list_processes") == []
        assert spy.deleted == [worker_id, "node_closure"]
        assert spy.heartbeats() == {}
        assert spy.methods_registry() == {}  # the master is unregistered too
