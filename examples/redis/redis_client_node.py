# %%
# Client of a node on Redis. Start `redis_node.py` first, in a terminal:
#   python redis_node.py --clean --node-id node_1
from distributed_processing.client import Client, serialize_python_call
from distributed_processing.exceptions import RemoteException
from distributed_processing.redis_connector import RedisConnector
from distributed_processing.worker import Worker

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
NAMESPACE = "tasks"
NODE_ID = "node_1"

client = Client(RedisConnector(REDIS_HOST, REDIS_PORT, REDIS_DB, NAMESPACE))

# %%
# Create three workers of type "worker1" on the node. queue=NODE_ID targets
# this node when several nodes share the namespace. Each call returns
# [pid, worker_type, worker_id] (a tuple on the node, a list after JSON).
z1 = client.rpc_sync("create_worker", ["worker1"], queue=NODE_ID)
z2 = client.rpc_sync("create_worker", ["worker1"], queue=NODE_ID)
z3 = client.rpc_sync("create_worker", ["worker1"], queue=NODE_ID)
print((z1, z2, z3))

# %%
# The alive worker subprocesses of the node.
client.rpc_sync("list_processes", [], queue=NODE_ID)

# %%
# Kill the second worker by pid: True. The node unregisters its methods
# and deletes its heartbeat.
client.rpc_sync("kill_process", [z2[0]], queue=NODE_ID)

# %%
client.rpc_sync("list_processes", [], queue=NODE_ID)

# %%
# The survivors, by queue; the killed worker is gone from the registry
# and from the heartbeats.
print(client.alive_workers())
print(client.connector.heartbeats())

# %%
# A request served by one of the spawned workers.
client.rpc_sync("add", [20, 22])

# %%
# A function sent to a spawned worker (py_eval queue).
client.rpc_sync_fn(lambda x, y: x * y, [6, 7])

# %%
# The constructor receives args and kwargs: here an explicit worker id.
client.rpc_sync(
    "create_worker", ["worker1", [], {"worker_id": "my_worker"}], queue=NODE_ID
)

# %%
# A worker type that the node was not started with: an import path
# "package.module:constructor". The subprocess imports it from disk, so a
# new module, or an edited one, is used by the next create_worker call
# without restarting the node. Here the node script itself is the module.
client.rpc_sync("create_worker", ["redis_node:worker1"], queue=NODE_ID)


# %%
# A constructor defined here and sent to the node (create_worker_fn). Needs
# the node started with --remote-constructors (allow_remote_constructors=True):
# it runs code sent by clients. The globals it uses travel with it; settings
# go as arguments.
def worker2(host, port, db, namespace, worker_id=None):
    server = Worker(RedisConnector(host, port, db, namespace), worker_id=worker_id)
    server.add_requests_queue("cola_3", {"triple": lambda x: 3 * x})
    server.update_methods_registry()
    return server


payload = serialize_python_call(worker2, [REDIS_HOST, REDIS_PORT, REDIS_DB, NAMESPACE])
client.rpc_sync("create_worker_fn", payload + ["worker2"], queue=NODE_ID)

# %%
# Registered under that name: listed by worker_types and usable by create_worker.
print(client.rpc_sync("worker_types", [], queue=NODE_ID))
print(
    client.rpc_sync(
        "create_worker",
        ["worker2", [REDIS_HOST, REDIS_PORT, REDIS_DB, NAMESPACE]],
        queue=NODE_ID,
    )
)
client.rpc_sync("triple", [14])

# %%
# An unknown worker type, or a constructor that fails, is an error on the
# node: a RemoteException here, with the reason.
try:
    client.rpc_sync("create_worker", ["nope"], queue=NODE_ID)
except RemoteException as e:
    print(e)

# %%
# Kill every worker: the pids killed.
client.rpc_sync("kill_all_processes", [], queue=NODE_ID)

# %%
# cleanup kills every worker and closes the master (unregistered, no
# heartbeat). The node process keeps running on a queue that is private
# now; stop it with Ctrl-C in its terminal.
client.rpc_sync("cleanup", [], queue=NODE_ID)
print(client.connector.heartbeats())
print(client.registry(update=True))
