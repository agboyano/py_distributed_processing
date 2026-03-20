import logging
from os import getenv
from time import sleep

from dotenv import load_dotenv

from distributed_processing.utils import fsnode, fsworker, serialize


def worker1(worker_id):
    server = fsworker(NS_PATH, clean=False, worker_id=worker_id)

    def info():
        rq = {}
        for k, v in server.requests_queues.items():
            rq[k] = set(v[0].keys())
        return server.worker_id, rq

    server.add_requests_queue(server.worker_id, {"info": info})

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
    server.add_python_eval()
    server.update_methods_registry()
    return server


if __name__ == "__main__":
    logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
    load_dotenv()
    NS_PATH = getenv("NS_PATH")
    workers_constructors = {"worker1": worker1}
    master = fsnode(
        NS_PATH,
        clean=True,
        worker_id="node_1",
        workers_constructors=workers_constructors,
    )
    master.run()
