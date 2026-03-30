import logging
from os import getenv
from time import sleep

from dotenv import load_dotenv

from distributed_processing.utils import fsnode, fsworker

logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
logging.getLogger("fs_structs").setLevel(logging.DEBUG)

def worker1(worker_id):
    server = fsworker(NS_PATH, clean=False, worker_id=worker_id)

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

    # Truco
    # Añado una cola que proporciona todas las funciones anteriores.
    # La llamo como el worker y le doy prioridad 100.
    # NO LA PUBLICO.
    # La utilizo para hacer llamadas directas al worker, saltándome las colas. 
    server.add_requests_queue(server.worker_id, dict(func_dict0, **func_dict1, **func_dict2), 100, False)
    # añado también el método eval_py_function a la cola anterior
    server.add_python_eval(server.worker_id)

    # creo cola "py_eval" con método eval_py_function
    server.add_python_eval()
    server.update_methods_registry()
    return server


if __name__ == "__main__":
    logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
    load_dotenv()
    NS_PATH = getenv("NS_PATH")
    MASTER_QUEUE = getenv("MASTER_QUEUE")
    workers_constructors = {"worker1": worker1}
    master = fsnode(
        NS_PATH,
        clean=True,
        worker_id=MASTER_QUEUE,
        workers_constructors=workers_constructors,
    )
    
    for i in range(3):
        master.exec_method("create_worker", ["worker1"], queue=MASTER_QUEUE)
    
    master.run()

