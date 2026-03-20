# %%
from time import sleep
import multiprocessing as mp
import dill
import uuid

from distributed_processing.utils import fsworker


#from multi import worker1

# %%
CURRO = True

if CURRO:
    NS_PATH ="G:\\fs_namespaces\\prueba_distribuida_multi"
    NS_PATH ="C:\\fs_namespaces\\prueba_distribuida_multi"

else:
    NS_PATH = "/home/augusto/python/notebooks/fs_namespaces/prueba_distribuida_multi"


# %%
import logging

logging.getLogger("distributed_processing").setLevel(logging.DEBUG)

# %%
# clean = True limpia namespace. Por defecto, False 
def _mp_worker_wrapper(serialized_func, serialized_args, serialized_kwargs, result_queue=None):
    """
    Global wrapper: deserialize function & arguments, call function,
    and optionally put the result into a queue.
    """
    func = dill.loads(serialized_func)
    args = dill.loads(serialized_args) if serialized_args is not None else ()
    kwargs = dill.loads(serialized_kwargs) if serialized_kwargs is not None else {}
    result = func(*args, **kwargs)
    if result_queue is not None:
        result_queue.put(result)

def create_process(func, args=None, kwargs=None, result_queue=None):
    """
    Create a Process that runs func(*args, **kwargs).
    If result_queue is given, the return value is put into it.
    """
    serialized_func = dill.dumps(func)
    serialized_args = dill.dumps(args) if args is not None else None
    serialized_kwargs = dill.dumps(kwargs) if kwargs is not None else None
    p = mp.Process(target=_mp_worker_wrapper, args=(serialized_func, serialized_args, serialized_kwargs, result_queue))
    return p

# %%
def worker1(worker_id):
    server = fsworker(NS_PATH, clean=False, worker_id=worker_id)

    def info():
        return server.worker_id, server.requests_queues

    server.add_requests_queue(server.worker_id, {"info":info})

    def add(x, y):
        return x + y

    def mul(x, y):
        return x * y

    def div(x, y):
        return x / y

    def lista(x, y):
        return [x, y]

    def tupla(x, y):
        return (x,y)

    def dic(x,y):
        return {"a":x, "b":[x,y]}

    func_dict1 = {"add" : add,
                "mul" : mul,
                "div" : div,
                "lista" : lista,
                "tupla" : tupla,
                "dic" :dic,
                "sleep": sleep}

    server.add_requests_queue("cola_1", func_dict1)

    def hola(nombre, calificativo="listo"):
        return f"Hola {nombre}, eres muy {calificativo}"

    func_dict2 = {"hola": hola}

    server.add_requests_queue("cola_2", func_dict2)
    server.add_python_eval()
    server.update_methods_registry()

    return server

workers_constructors = {"worker1":worker1}
processes = []
active_workers ={}


def create_worker(worker_type, worker_id, id, shared_dict):
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


# %%
if __name__ == '__main__':
    manager = mp.Manager()
    active_workers = manager.dict()

    def create_process(worker_type, worker_id=None):
        id = uuid.uuid4()
        p = mp.Process(target=create_worker, args=[worker_type, worker_id, id, active_workers])
        p.start()
        sleep(5)
        processes.append((p, p.pid, worker_type, active_workers.get(id, "error")))
        return p.pid

    master_funcs = {"create_worker":create_process,
                "list_processes":list_processes,
                "kill_process":kill_process,
                "kill_processes":kill_processes}

    
    master = fsworker(NS_PATH, clean=True, worker_id ="node_1")
    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()


# %%
    master.run()
