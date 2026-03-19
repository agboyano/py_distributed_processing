import multiprocessing as mp
import uuid
from time import sleep

import dill

from .client import Client
from .filesystem_connector import FileSystemConnector
from .serializers import DummySerializer
from .worker import Worker

dill.settings["recurse"] = True # importante


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


def _create_worker(serialized_worker_constructor, worker_id, id, shared_dict):
    worker_constructor = dill.loads(serialized_worker_constructor)
    worker = worker_constructor(worker_id)
    shared_dict[id] = worker.worker_id
    worker.run()


def fsnode(
    NS_PATH, clean=False, with_watchdog=True, worker_id=None, workers_constructors={}
):
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

    def kill_process(pid):
        ps = [
            (p, worker_id)
            for p, _, _, worker_id in processes
            if p.is_alive() and p.pid == pid
        ]
        if len(ps) != 1:
            return False
        else:
            p, worker_id = ps[0]
            p.terminate()
            master.connector.unregister_methods(worker_id)
            return True

    def kill_processes(pids):
        ps = [
            (p, worker_id)
            for p, _, _, worker_id in processes
            if p.is_alive() and p.pid in pids
        ]
        if len(ps) == 0:
            return False
        else:
            for p, worker_id in ps:
                p.terminate()
                master.connector.unregister_methods(worker_id)
            return True

    def create_process(worker_type, worker_id=None):
        id = uuid.uuid4()
        worker_constructor = workers_constructors[worker_type]
        serialized_worker_constructor = dill.dumps(worker_constructor)

        p = mp.Process(
            target=_create_worker,
            args=[serialized_worker_constructor, worker_id, id, active_workers],
        )
        p.start()
        while id not in active_workers:
            sleep(0.1)
        processes.append((p, p.pid, worker_type, active_workers.get(id, "error")))
        return p.pid

    master_funcs = {
        "create_worker": create_process,
        "list_processes": list_processes,
        "kill_process": kill_process,
        "kill_processes": kill_processes,
    }

    master = fsworker(NS_PATH, clean=clean, worker_id=worker_id)
    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()

    return master
