# %%
import random
import time
from os import getenv

import fs_structs
from dotenv import load_dotenv

from distributed_processing.async_result import gather
from distributed_processing.utils import fsclient

# %%
load_dotenv()
NS_PATH = getenv("NS_PATH")


# %%
import logging

# logging.getLogger("distributed_processing").setLevel(logging.DEBUG)
# logging.getLogger("distributed_processing.filesystem_connector").setLevel(logging.DEBUG)

# %%
"""
fs_connector = FileSystemConnector(NS_PATH)
fs_connector.with_watchdog=True
fs_connector.pop_watchdog_timeout = 10

client = Client(DummySerializer(), fs_connector, check_registry="cache")
"""

client = fsclient(NS_PATH)

# %%
client.update_registry_cache()

# %%
ns = fs_structs.structs.FSNamespace(NS_PATH)

# %%
ns.registry.keys()

# %%
z1 = client.rpc_sync("create_worker", ["worker1"])
z2 = client.rpc_sync("create_worker", ["worker1"])
z3 = client.rpc_sync("create_worker", ["worker1"])

(z1, z2, z3)

# %%
client.rpc_sync("list_processes", [])

# %%
client.rpc_sync("kill_process", [z2])

# %%
print(client.rpc_sync("list_processes", []))

# %%
time.sleep(5)
print("**************************************")

# %%
y = client.rpc_async("add", [1, 0])

# %%
y.get()

# %%
client.check_registry = "always"

try:
    y = client.rpc_async("fake", [1, 0])
except Exception as e:
    print(e)

# %%
client.check_registry = "never"  # use queue client.default_queue, by default "default"
client.set_default_queue("cola_1")

y = client.rpc_async("fake", [1, 0])

try:
    y.get()
except Exception as e:
    print(e)


# %%
client.check_registry = "never"
client.set_default_queue("cola_1")

y = client.rpc_async("fake", [1, 0])

y.safe_get(default="Esto es un error")

# %%
client.check_registry = "cache"

# %%
x = client.rpc_async("div", [1, 0])

# %%
try:
    x.get()
except Exception as e:
    print(e)

# %%
# x = client.rpc_sync("div", [1, 0])

# %%
client.rpc_sync("add", [1, 1])


# %%
def f(x, y):
    return x + y


y = client.rpc_async_fn(f, [1, 2.0])

# %%
y.get()

# %%
client.rpc_sync_fn(f, [3.0, 2.0])

# %%
fs = []
tp = []
N = 10
for i in range(N):
    fn = random.choice(("add", "mul", "div", "lista", "tupla", "dic"))
    t = (fn, [random.random(), random.random()], {})
    tp.append(t)
    fs.append(client.rpc_async(t[0], t[1]))

t = ("sleep", [10], {})
tp.append(t)
fs.append(client.rpc_async(t[0], t[1]))


# %%
resultados = [f.get() for f in fs]
resultados

# %%
fs = client.rpc_multi_async(tp)

# %%
# AsynResult.status updates the information every time it is called.
# 'PENDING' state should be assumed as transitory.
# If there are responses available in the queue or in the cache
# status or pending() updates the AsyncResult object.

[f.status for f in fs]

# %%
client.wait_responses(timeout=2)

# %%
[f.status for f in fs]

# %%
client.wait_responses()

# %%
# AsynResult.status updates information

[f.status for f in fs]

# %%
try:
    client.wait_responses(["kk", "qq"])
except ValueError as e:
    print(e)

# %%
client._update_cache_with_all_available_responses()

# %%
client.pending

# %%
[f.get() for f in fs]

# %%
fs = client.rpc_batch_async(tp)

# %%
[f.get() for f in fs]

# %%
client.rpc_batch_sync(tp)

# %%
fs = []
tp = []
N = 10
for i in range(N):
    fn = random.choice(("add", "mul", "div", "fake"))
    t = (fn, [random.random(), random.random()], {})
    tp.append(t)

# %%
tp

# %%
client.check_registry = "never"
client.set_default_queue("cola_1")

fs = client.rpc_batch_async(tp)

# %%
[f.safe_get() for f in fs]

# %%
client.responses

# %%
client.check_registry = "cache"

# %%
try:
    x = client.rpc_batch_sync(tp, timeout=5)
except Exception as e:
    print(e)

# %%
try:
    x = client.rpc_batch_sync(tp, timeout=5)
except Exception as e:
    print(e)

# %%
client.check_registry = "never"
client.set_default_queue("cola_1")

x = client.rpc_async("kk", [1, 0])

# %%
try:
    x.get()
except Exception as e:
    print(e)

# %%
y = client.rpc_async("add", [1, 0])

# %%
y.get(5)


# %%
def f(x, y):
    return x + y


client.check_registry = (
    "never"  # "never" use queue "default" don't work for rpc_async_fn or rpc_sync_fn
)
y = client.rpc_async_fn(f, [1, 2.0])
try:
    print(y.get())
except Exception as e:
    print(e)

# %%
client.check_registry = "cache"
y = client.rpc_async_fn(f, [1, 2.0])

# %%
y.get()

# %%
[k for k in client.responses.keys()]

# %%
client.clean_used()

# %%
[k for k in client.responses.keys()]

# %%
resultados

# %%
client.rpc_sync_fn(print, ["hola"])

# %%
fs = []
tp = []
N = 10
for i in range(N):
    fn = random.choice(("sleep", "sleep"))
    t = (fn, [15], {})
    print(t)
    tp.append(t)

# %%
client.check_registry = "never"
fs = client.rpc_multi_async(tp, retry=True)

# %%
from time import time


def gather_kk(arl, timeout=None, step=5, max_dts=0):
    # Update to reset delta times
    pending_ars = [ar for ar in arl if not (ar.ok() or ar.failed())]
    pending_ids = [ar.id for ar in pending_ars]
    if fs[0]._client.wait_responses(pending_ids, timeout=0) == []:
        return

    N = len(arl)

    max_dts = max_dts

    t_0 = time()

    i = 0
    # ok() and failed() don't update.
    # pending() updates, every time is called, all the ids
    # that have a response available in the queue
    pending_ars = [ar for ar in arl if not (ar.ok() or ar.failed())]
    pending_ars0 = pending_ars
    pending_ids = [ar.id for ar in pending_ars]
    t_prev = time()
    n_pending_prev = len(pending_ars)
    n_pending_0 = n_pending_prev
    while (time() - t_0) <= timeout:
        try:
            if fs[0]._client.wait_responses(pending_ids, timeout=5) == []:
                return
        except TimeoutError:
            pass
        dts = [
            ar.finished_time - t_0 for ar in pending_ars0 if (ar.ok() or ar.failed())
        ]
        if len(dts) > 0:
            # max_dts = max(max_dts, max(dts))
            max_dts = max(dts)
        pending_ars = [ar for ar in arl if not (ar.ok() or ar.failed())]
        pending_ids = [ar.id for ar in pending_ars]
        if len(pending_ars) < n_pending_prev:
            t_prev = time()
            n_pending_prev = len(pending_ars)

        avg = (
            (time() - t_0) / (n_pending_0 - len(pending_ars))
            if (n_pending_0 - len(pending_ars)) > 0
            else 0
        )

        print(
            f"{i}: seconds {time() - t_0}s, AR recovered {N - len(pending_ars)}, \
                AR left {len(pending_ars)}, max delta {max_dts}, avg {avg}"
        )
        i += 0


# %%
gather(fs, 30, 5)

# %%
[f.status for f in fs]

# %%
[(time() if f.finished_time is None else f.finished_time) - f.creation_time for f in fs]

# %%
[f.finished_time for f in fs]

# %%
client.pending

# %%
fs[3].status

# %%
fs[4].retry()

# %%
# [f.retry() for f in fs if not f.done()]
# no hace falta chequear si está pendiente.
[f.retry() for f in fs]

# %%
client.check_registry = "always"
<<<<<<< HEAD
client.all_queues_for_method("info")
=======
client._all_queue_refs_for_method("info")
>>>>>>> wip

# %%
client.connector.all_queues_for_method("info")

# %%
client.update_registry_cache()

# %%
client.check_registry = "Never"
<<<<<<< HEAD
client.all_queues_for_method("hola")
=======
client._all_queue_refs_for_method("hola")
>>>>>>> wip

# %%
client.check_registry = "always"
a = client.rpc_async("info")

# %%
client.rpc_sync("div", [2, 1], timeout=10)

# %%
x = a.get()

# %%
client._registry



# %%
def registry_one_step(x):
    worker_id = x[0]
    registry = {}
    for queue, methods in x[1].items():
        for method in methods:
            if method in registry:
                registry[method].append(queue)
            else:
                registry[method] = [queue]
    return registry


def queues_workers_one_step(x):
    worker_id = x[0]
    registry = {}
    for queue in x[1]:
        if queue in registry:
            registry[queue].append(worker_id)
        else:
            registry[queue] = [worker_id]
    return registry


def all_workers_for_queue():
    # all requests queues for "info" method
    # One queue per worker

    rr = {}
    qq = {}
    all_worker_queues = client.connector.all_queues_for_method("info")
    for worker in all_worker_queues:
        try:
            x = client.rpc_sync("info", timeout=10)
        except TimeoutError:
            x = None

        if x is not None:
            for method, qs in registry_one_step(x).items():
                if method not in rr:
                    rr[method] = qs
                else:
                    rr[method] += qs
            for q, ws in queues_workers_one_step(x).items():
                if q not in qq:
                    qq[q] = ws
                else:
                    qq[q] += ws
    return rr, qq


# %%
registry_one_step(x)

# %%
queues_workers_one_step(x)

# %%
r, q = all_workers_for_queue()

# %%
r

# %%
q

# %%
t = (1, 2, 3, 4)

# %%
t[:6]

# %%
t[:5]

# %%
