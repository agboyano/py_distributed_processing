from __future__ import annotations

import atexit
import logging
import multiprocessing as mp
import uuid
from collections import namedtuple
from time import sleep, time
from typing import Any

import dill

from .client import Client
from .filesystem_connector import FileSystemConnector
from .serializers import DummySerializer
from .worker import Worker

# Serialize the whole closure of the functions sent between processes.
dill.settings["recurse"] = True

logger = logging.getLogger(__name__)


def fsclient(
    NS_PATH: str,
    check_registry: str = "cache",
    with_watchdog: bool = True,
    pop_watchdog_timeout: float = 10,
) -> Client:
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = with_watchdog
    fs_connector.pop_watchdog_timeout = pop_watchdog_timeout
    return Client(DummySerializer(), fs_connector, check_registry=check_registry)


def fsworker(
    NS_PATH: str,
    clean: bool = False,
    with_watchdog: bool = True,
    worker_id: str | None = None,
    watchdog_timeout: float = 60,
) -> Worker:
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = with_watchdog
    fs_connector.pop_watchdog_timeout = watchdog_timeout
    if clean:
        fs_connector.clean_namespace()

    return Worker(DummySerializer(), fs_connector, worker_id=worker_id)


def serialize(x: Any) -> bytes:
    return dill.dumps(x)


def deserialize(x: bytes) -> Any:
    return dill.loads(x)


def _create_worker(serialized_worker_constructor, args, kwargs, id, shared_dict):
    worker_constructor = deserialize(serialized_worker_constructor)
    worker = worker_constructor(*args, **kwargs)
    shared_dict[id] = worker.worker_id
    worker.run()


def fsnode(
    NS_PATH: str,
    clean: bool = False,
    with_watchdog: bool = True,
    worker_id: str | None = None,
    workers_constructors: dict | None = None,
    watchdog_timeout: float = 60,
    creation_processes_timeout: float = 60,
) -> Worker:
    workers_constructors = {} if workers_constructors is None else workers_constructors
    WorkerProcess = namedtuple(
        "WorkerProcess", ["p", "pid", "worker_type", "worker_id"]
    )
    processes = []
    manager = mp.Manager()
    active_workers = manager.dict()

    def list_processes():
        return [
            (pid, worker_type, worker_id)
            for p, pid, worker_type, worker_id in processes
            if p.is_alive()
        ]

    def _kill_process(wp):
        if wp.p.is_alive():
            killed = False
            # logger.info(f"Cleaning and unregistering process {wp.pid} with worker_id {wp.worker_id} of type {wp.worker_type} for {master.worker_id}.")
            wp.p.terminate()  # Send SIGTERM
            wp.p.join(timeout=30)  # Better terminate.
            if wp.p.is_alive():
                wp.p.kill()  # Force kill
                killed = True
                # logger.info(f"Killing process {wp.pid}.")
            master.connector.unregister_methods(wp.worker_id)
            return True, killed
        return False, False

    def kill_process(pid):
        wps = [wp for wp in processes if wp.p.is_alive() and wp.pid == pid]
        if len(wps) != 1:
            return False
        else:
            return _kill_process(wps[0])

    def kill_processes(pids):
        return [kill_process(pid) for pid in pids]

    def kill_all_processes():
        kp = []
        for wp in processes:
            try:
                kp.append(_kill_process(wp))
                sleep(1)
            except Exception:
                kp.append(None)
        return kp

    def create_process(worker_type, args=None, kwargs=None):
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        id = uuid.uuid4()
        worker_constructor = workers_constructors[worker_type]
        serialized_worker_constructor = serialize(worker_constructor)

        p = mp.Process(
            target=_create_worker,
            args=[serialized_worker_constructor, args, kwargs, id, active_workers],
        )
        p.daemon = False  # will die when parent dies
        p.start()
        start = time()
        while (
            id not in active_workers and (time() - start) < creation_processes_timeout
        ):
            sleep(0.1)
        if id not in active_workers:
            # The process never registered itself: it is not in `processes`
            # yet, so terminate it directly instead of using kill_process.
            p.terminate()
            p.join(timeout=30)
            if p.is_alive():
                p.kill()
            return None, None, None
        wp = WorkerProcess(p, p.pid, worker_type, active_workers.get(id, "error"))
        processes.append(wp)
        return p.pid, worker_type, wp.worker_id

    def cleanup():
        kp = kill_all_processes()
        master.unregister()
        return kp

    master_funcs = {
        "create_worker": create_process,
        "list_processes": list_processes,
        "kill_process": kill_process,
        "kill_processes": kill_processes,
        "kill_all_processes": kill_all_processes,
        "cleanup": cleanup,
    }

    master = fsworker(
        NS_PATH,
        clean=clean,
        with_watchdog=with_watchdog,
        worker_id=worker_id,
        watchdog_timeout=watchdog_timeout,
    )
    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()
    atexit.register(cleanup)

    return master
