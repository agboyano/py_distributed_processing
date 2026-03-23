import atexit
import multiprocessing as mp
import uuid
from collections import namedtuple
from time import sleep

import dill

from .client import Client
from .filesystem_connector import FileSystemConnector
from .serializers import DummySerializer
from .worker import Worker

dill.settings["recurse"] = True  # importante

import logging

logger = logging.getLogger(__name__)


def fsclient(
    NS_PATH, check_registry="Cache", with_watchdog=True, pop_watchdog_timeout=10
):
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = True
    fs_connector.pop_watchdog_timeout = 10
    return Client(DummySerializer(), fs_connector, check_registry="cache")


def fsworker(NS_PATH, clean=False, with_watchdog=True, worker_id=None):
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = True
    if clean:
        fs_connector.clean_namespace()

    return Worker(DummySerializer(), fs_connector, worker_id=worker_id)


def serialize(x):
    return dill.dumps(x)


def deserialize(x):
    return dill.loads(x)


def _create_worker(serialized_worker_constructor, worker_id, id, shared_dict):
    worker_constructor = deserialize(serialized_worker_constructor)
    worker = worker_constructor(worker_id)
    shared_dict[id] = worker.worker_id
    worker.run()


def fsnode(
    NS_PATH, clean=False, with_watchdog=True, worker_id=None, workers_constructors={}
):
    WorkerProcess = namedtuple(
        "WorkerProcess", ["p", "pid", "worker_type", "worker_id"]
    )
    processes = []
    workers_constructors = workers_constructors
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
            #logger.info(f"Cleaning and unregistering process {wp.pid} with worker_id {wp.worker_id} of type {wp.worker_type} for {master.worker_id}.")
            wp.p.terminate()  # Send SIGTERM
            wp.p.join(timeout=30)  # Better terminate.
            if wp.p.is_alive():
                wp.p.kill()  # Force kill
                #logger.info(f"Killing process {wp.pid}.")
            sleep(1)
            master.connector.unregister_methods(wp.worker_id)
            return True
        return False

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
            except:
                kp.append(None)
        return kp

    def create_process(worker_type, worker_id=None):
        id = uuid.uuid4()
        worker_constructor = workers_constructors[worker_type]
        serialized_worker_constructor = serialize(worker_constructor)

        p = mp.Process(
            target=_create_worker,
            args=[serialized_worker_constructor, worker_id, id, active_workers],
        )
        p.daemon = False  # will die when parent dies
        p.start()
        while id not in active_workers:
            sleep(0.1)
        wp = WorkerProcess(p, p.pid, worker_type, active_workers.get(id, "error"))
        processes.append(wp)
        return p.pid

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

    master = fsworker(NS_PATH, clean=clean, worker_id=worker_id)
    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()
    atexit.register(cleanup)

    return master
