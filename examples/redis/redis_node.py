"""A node on Redis: a master worker that starts, lists and kills worker
subprocesses on request (see `distributed_processing.node`).

Run it in a terminal and drive it from `redis_client_node.py`:

    python redis_node.py --clean --node-id node_1 --workers 2
"""

import argparse
import logging
from time import sleep

from distributed_processing.redis_connector import RedisConnector
from distributed_processing.utils import redisnode
from distributed_processing.worker import Worker

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
NAMESPACE = "tasks"
NODE_ID = "node_1"


def worker1(worker_id=None):
    """Worker constructor. It runs inside the subprocess, so it builds its
    own connector. The connection settings are module globals: dill
    serializes their values together with the function."""
    server = Worker(
        RedisConnector(REDIS_HOST, REDIS_PORT, REDIS_DB, NAMESPACE), worker_id=worker_id
    )

    def info():
        rq = {}
        for k, v in server.requests_queues.items():
            rq[k] = sorted(v[0].keys())
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

    server.add_requests_queue("cola_2", {"hola": hola})
    # SECURITY: eval_py_function runs any code sent by clients.
    server.add_python_eval()
    server.update_methods_registry()
    return server


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, default=REDIS_HOST, help="Redis server address"
    )
    parser.add_argument(
        "-p", "--port", type=int, default=REDIS_PORT, help="Redis server port"
    )
    parser.add_argument("-db", type=int, default=REDIS_DB, help="Redis server DB")
    parser.add_argument(
        "-n", "--namespace", type=str, default=NAMESPACE, help="Namespace to use"
    )
    parser.add_argument(
        "--node-id", type=str, default=NODE_ID, help="Node id (its queue name)"
    )
    parser.add_argument(
        "--workers", type=int, default=0, help="Workers to start before serving"
    )
    parser.add_argument("--clean", action="store_true", help="Clean namespace")
    # SECURITY: with this flag the node runs constructors sent by clients
    # (create_worker_fn), like eval_py_function. Trusted infrastructure only.
    parser.add_argument(
        "--remote-constructors", action="store_true", help="Enable create_worker_fn"
    )
    args = parser.parse_args()

    # Subprocesses do not inherit the logging configuration; this only
    # applies to the node process.
    logging.basicConfig(level=logging.INFO)
    REDIS_HOST, REDIS_PORT, REDIS_DB, NAMESPACE = (
        args.host,
        args.port,
        args.db,
        args.namespace,
    )

    print(
        f"Node {args.node_id} on Redis {args.host}:{args.port}, DB {args.db}, namespace {args.namespace}"
    )
    master = redisnode(
        args.host,
        args.port,
        args.db,
        args.namespace,
        clean=args.clean,
        worker_id=args.node_id,
        workers_constructors={"worker1": worker1},
        allow_remote_constructors=args.remote_constructors,
    )

    for _ in range(args.workers):
        master.exec_method("create_worker", ["worker1"], queue=args.node_id)

    # run() stops with a traceback on the first connector error (good while
    # developing). For a node deployed as a service use run_forever().
    master.run()
