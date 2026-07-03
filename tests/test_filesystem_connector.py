import threading

import pytest

pytest.importorskip("fs_structs")

from distributed_processing.client import Client
from distributed_processing.filesystem_connector import FileSystemConnector
from distributed_processing.serializers import DummySerializer
from distributed_processing.worker import Worker


@pytest.mark.integration
class TestFileSystemConnector:
    def test_enqueue_pop_round_trip(self, tmp_path):
        conn = FileSystemConnector(str(tmp_path))
        conn.enqueue("some_queue", {"x": 1})
        result = conn.pop("some_queue", timeout=0)
        assert result == ("some_queue", {"x": 1})

    def test_pop_timeout_returns_none(self, tmp_path):
        conn = FileSystemConnector(str(tmp_path))
        assert conn.pop("empty_queue", timeout=0) is None

    def test_registry_round_trip(self, tmp_path):
        conn = FileSystemConnector(str(tmp_path))
        w = Worker(DummySerializer(), conn)
        w.add_requests_queue("q", {"add": lambda a, b: a + b})
        w.update_methods_registry()

        conn_client = FileSystemConnector(str(tmp_path))
        queue_ref = conn.get_requests_queue("q")
        registry = conn_client.methods_registry()
        assert registry == {"add": [queue_ref]}
        # workers_registry is keyed by the long queue ref; Client.registry()
        # is the one that translates refs back to simple names.
        assert conn_client.workers_registry() == {queue_ref: [w.worker_id]}

        w.unregister()
        assert conn_client.methods_registry() == {}

    def test_unknown_method_has_no_queues(self, tmp_path):
        conn = FileSystemConnector(str(tmp_path))
        assert conn.all_queues_for_method("does_not_exist") == []
        assert conn.random_queue_for_method("does_not_exist") is None

    def test_fsworker_and_fsclient_honor_their_params(self, tmp_path):
        from distributed_processing.utils import fsclient, fsworker

        w = fsworker(str(tmp_path), with_watchdog=False, watchdog_timeout=7)
        assert w.connector.with_watchdog is False
        assert w.connector.pop_watchdog_timeout == 7

        c = fsclient(
            str(tmp_path),
            check_registry="never",
            with_watchdog=False,
            pop_watchdog_timeout=3,
        )
        assert c.connector.with_watchdog is False
        assert c.connector.pop_watchdog_timeout == 3
        assert c.check_registry == "never"

    def test_rpc_round_trip(self, tmp_path):
        worker_conn = FileSystemConnector(str(tmp_path))
        w = Worker(DummySerializer(), worker_conn)
        w.add_requests_queue("q", {"add": lambda a, b: a + b})
        w.update_methods_registry()

        worker_thread = threading.Thread(
            target=w.run, kwargs={"timeout": 10}, daemon=True
        )
        worker_thread.start()

        client_conn = FileSystemConnector(str(tmp_path))
        client = Client(DummySerializer(), client_conn, check_registry="cache")
        assert client.rpc_sync("add", [20, 22], timeout=10) == 42
