from .client import Client
from .filesystem_connector import FileSystemConnector
from .serializers import DummySerializer
from .worker import Worker
from time import sleep
import multiprocessing as mp
import uuid

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


def create_worker(worker_constructor, worker_id, id, shared_dict):
    worker = worker_constructor(worker_id)
    shared_dict[id] = worker.worker_id
    worker.run()


def fsnode(NS_PATH, clean=False, with_watchdog=True, worker_id=None, workers_constructors={}):
    processes = []
    workers_constructors = workers_constructors

    def create_worker_kk(worker_type, worker_id, id, shared_dict):
        if worker_type in workers_constructors:
            worker = workers_constructors[worker_type](worker_id)
            shared_dict[id] = worker.worker_id
            worker.run()

    def list_processes():
        return [(pid, worker_type, worker_id) for p, pid, worker_type, worker_id in processes if p.is_alive()]

    def kill_process(pid):
        ps = [p for p, _, _, _ in processes if p.is_alive() and p.pid==pid]
        if len(ps)!=1:
            return False
        else:
            ps[0].terminate()
            return True

    def kill_processes(pids):
        ps = [p for p, _, _, _ in processes if p.is_alive() and p.pid in pids]
        if len(ps) == 0:
            return False
        else:
            for p in ps:
                p.terminate()
            return True
        
    manager = mp.Manager()
    active_workers = manager.dict()

    def create_process(worker_type, worker_id=None):
        id = uuid.uuid4()
        worker_constructor = workers_constructors[worker_type]
        p = mp.Process(target=create_worker, args=[worker_constructor, worker_id, id, active_workers])
        p.start()
        sleep(5)
        processes.append((p, p.pid, worker_type, active_workers.get(id, "error")))
        return p.pid

    master_funcs = {"create_worker":create_process,
                "list_processes":list_processes,
                "kill_process":kill_process,
                "kill_processes":kill_processes}

    
    master = fsworker(NS_PATH, clean=clean, worker_id=worker_id)
    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()

    return master
