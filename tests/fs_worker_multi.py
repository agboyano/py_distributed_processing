from os import getenv
from time import sleep

from dotenv import load_dotenv

from distributed_processing.utils import fsnode, fsworker

# logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
# logging.getLogger("fs_structs").setLevel(logging.DEBUG)


def worker1(worker_id=None, watchdog_timeout=60):
    server = fsworker(NS_PATH, clean=False, worker_id=worker_id, watchdog_timeout=60)

    def info():
        rq = {}
        for k, v in server.requests_queues.items():
            rq[k] = set(v[0].keys())
        return server.worker_id, rq

    func_dict0 = {"info": info}

    server.add_requests_queue("cola_0", func_dict0)

    def add(x, y):
        return x + y

    def mul(x, y):
        return x * y

    def div(x, y):
        return x / y

    def lista(x, y):
        return [x, y]

    def tupla(x, y):
        return (x, y)

    def dic(x, y):
        return {"a": x, "b": [x, y]}

    func_dict1 = {
        "add": add,
        "mul": mul,
        "div": div,
        "lista": lista,
        "tupla": tupla,
        "dic": dic,
        "sleep": sleep,
    }

    server.add_requests_queue("cola_1", func_dict1)

    def hola(nombre, calificativo="listo"):
        return f"Hola {nombre}, eres muy {calificativo}"

    func_dict2 = {"hola": hola}

    server.add_requests_queue("cola_2", func_dict2)

    # Trick
    # Add a queue that offers every function above.
    # Named after the worker, with priority 100.
    # NOT PUBLISHED.
    # Used to call the worker directly, bypassing the queues.
    server.add_requests_queue(
        server.worker_id, dict(func_dict0, **func_dict1, **func_dict2), 100, False
    )
    # also add the eval_py_function method to that queue
    server.add_python_eval(server.worker_id)

    # create the "py_eval" queue with the eval_py_function method
    server.add_python_eval()
    server.update_methods_registry()
    return server


if __name__ == "__main__":
    # logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
    load_dotenv()
    NS_PATH = getenv("NS_PATH")
    MASTER_QUEUE = getenv("MASTER_QUEUE")
    workers_constructors = {"worker1": worker1}
    master = fsnode(
        NS_PATH,
        clean=True,
        worker_id=MASTER_QUEUE,
        workers_constructors=workers_constructors,
        watchdog_timeout=30,
    )

    for _ in range(3):
        master.exec_method("create_worker", ["worker1", [None, 20]], queue=MASTER_QUEUE)

    master.run()
