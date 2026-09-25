# distributed-processing

Lightweight distributed computing library on top of message queues, with an
RPC protocol inspired by [JSON-RPC 2.0](https://www.jsonrpc.org/specification).

A **worker** registers functions and listens on one or more request queues;
a **client** discovers which methods are available (registry), sends requests
and collects responses synchronously or asynchronously (`AsyncResult`). The
transport is pluggable via **connectors**: Redis or a shared filesystem
(NFS, local disk, etc.).

## Features

- Synchronous and asynchronous RPC (`rpc_sync`, `rpc_async`, `AsyncResult`).
- Batch requests (a single message, a single worker) and multi requests
  (spread across workers): `rpc_batch_*`, `rpc_multi_*`.
- Method registry: clients discover which queues serve each method
  (`check_registry="cache" | "always" | "never"`, case-insensitive; any
  other value raises `ValueError`). An explicit `queue` on any request is
  used as is; `default_queue` is the target of `"never"`.
- Queues with priorities; queues of equal priority are shuffled on each
  iteration.
- Notifications (requests without a response), optional acks and retries
  (`AsyncResult.retry`). `gather(fs, timeout)` waits for AsyncResults of
  several clients at once and returns the ones still pending; with
  `retry_lost=True` it resends the requests that a worker took and never
  answered because it died.
- Sending arbitrary Python functions serialized with `dill`
  (`rpc_async_fn` + `Worker.add_python_eval`). **See security note.**
- Connectors: Redis (`RedisConnector`) and filesystem
  (`FileSystemConnector`, based on `fs_structs`, waiting on watchdog
  events or polling). The connector owns the wire encoding: Redis takes any
  `dumps`/`loads` pair (JSON by default; `pickle`, `msgpack`...), the
  filesystem connector stores with an `fs_structs` serializer (joblib by
  default).
- Shared variables: `set_variable` / `get_variable` on the connector, the
  client and the worker, to publish a parameter once instead of sending it
  with every request.
- Worker heartbeats: `run` / `run_forever` beat every 10 s from a thread;
  `client.alive_workers()` tells which registered workers are alive and
  `client.prune_dead_workers()` removes the dead ones from the registry.

## Installation

```bash
pip install .              # core (dill)
pip install .[redis]       # + Redis connector
pip install .[fs]          # + filesystem connector (fs_structs, watchdog)
pip install .[dev]         # + pytest
```

`fs_structs` is a sibling project not published on PyPI; install it locally,
for example: `pip install -e ../fs_structs`.

## Quick start

Worker (one process):

```python
from distributed_processing import Worker
from distributed_processing.redis_connector import RedisConnector

def add(a, b):
    return a + b

worker = Worker(RedisConnector("localhost"))
worker.add_requests_queue("my_queue", {"add": add})
worker.update_methods_registry()
worker.run()          # listens indefinitely; run(timeout=60) to bound it
```

For a long-running service use `run_forever()` instead of `run()`: connector
errors (a shared drive that stops responding, a lock timeout, a corrupt
message) are logged and retried with exponential backoff instead of ending
the process. `worker.stop()` (from a registered function or another thread)
makes it return; use a `with Worker(...)` block, or call `close()`, so the
worker is removed from the registry on shutdown. Both loops send a heartbeat
every 10 s from a thread, so clients can tell a dead worker from a busy one
(see *Heartbeats*).

```python
with Worker(RedisConnector("localhost")) as worker:
    worker.add_requests_queue("my_queue", {"add": add})
    worker.update_methods_registry()
    worker.run_forever(backoff=(1, 60))   # retry forever; max_consecutive_errors=N to give up
```

Client (another process):

```python
from distributed_processing import Client
from distributed_processing.redis_connector import RedisConnector

client = Client(RedisConnector("localhost"))

client.rpc_sync("add", [1, 2])          # → 3, blocking

f = client.rpc_async("add", [20, 22])   # AsyncResult
f.get(timeout=10)                       # → 42

fs = client.rpc_multi_async([("add", [i, i], None) for i in range(100)])
[f.safe_get(timeout=60) for f in fs]    # spread across the workers
```

With the filesystem connector you only need to share a directory:

```python
from distributed_processing.utils import fsworker, fsclient

worker = fsworker("/shared/path/ns")   # + add_requests_queue + run()
client = fsclient("/shared/path/ns")
```

To launch a *node*, a master worker that starts, lists and kills worker
subprocesses on request (`create_worker`, `list_processes`, `kill_process`...
via RPC), see `distributed_processing.utils.node`: `fsnode(...)` builds one on
a shared directory and `redisnode(...)` on Redis. Worker subprocesses always
start with `spawn`, so a constructor defined in a notebook works; each
constructor builds its own connector. A worker type may also be an import
path (`create_worker("pkg.module:make_worker")`): the subprocess imports it
from disk, so new types and new versions need no node restart. With
`allow_remote_constructors=True` the node also accepts constructors sent by
clients (`create_worker_fn`), which runs their code: trusted infrastructure
only.

## Connectors and serialization

`Client` and `Worker` know nothing about the wire format: they hand Python
objects (dicts and lists) to the connector and get Python objects back. Each
connector decides how to store them:

- `RedisConnector(..., serializer=None)`: `serializer` is any object with
  `dumps(obj) -> bytes` and `loads(bytes) -> obj`, `JsonSerializer()` by
  default. `distributed_processing.serializers` also provides
  `PickleSerializer(protocol=None)` (keeps Python types) and
  `JoblibSerializer(compress=0)` (efficient for NumPy/pandas, needs `joblib`);
  the `pickle`, `dill` and `msgpack` modules work as they are too
  (`RedisConnector("localhost", serializer=pickle)`). Pickle-based
  serializers execute code from the data: trusted infrastructure only. The
  connection uses `decode_responses=False`, so binary formats are safe; a
  message that cannot be decoded is logged and skipped.
- `FileSystemConnector(base_path, temp_dir=None, serializer=...)`: `serializer`
  is an `fs_structs` serializer (`joblib_serializer` by default, also
  `pickle_serializer` and `json_serializer`), the same one used for the
  registry.

Every client and worker on a namespace must use the same serializer.

### Shared variables

Besides queues, a namespace holds a small key/value store that every client
and worker can read and write. `Client` and `Worker` expose it with the same
four methods as the connector:

```python
client.set_variable("valuation_date", "2026-09-18")
client.get_variable("valuation_date")            # '2026-09-18'
client.get_variable("missing", default=0)        # 0
client.variables()                               # ['valuation_date']
client.delete_variable("valuation_date")         # True

# On the worker side a registered function reads it through a closure:
worker.add_function("q", "price", lambda isin: price(isin, worker.get_variable("valuation_date")))
```

Rules:

- Values are **copies**: they go through the connector's serializer (so on
  Redis with the default `JsonSerializer` they must be JSON-encodable, as any
  request). Mutating what `get_variable` returns changes nothing; call
  `set_variable` again.
- Each call is atomic on its own and the **last write wins**: no lock and no
  expiry. Two processes doing `set_variable(name, get_variable(name) + 1)` at
  the same time may lose an update. For that use
  `update_variable(name, fn, default=...)`: it runs `fn(current)` and stores
  the result while holding a lock on that variable (a lock directory on the
  filesystem, a Redis lock), and returns the new value:

  ```python
  worker.update_variable("done", lambda n: n + 1, default=0)
  ```

  A variable that is not set raises `KeyError` unless `default` is given
  (`None` counts as given). Keep `fn` pure and quick (it runs with the lock
  held), and never write a variable that is updated this way with
  `set_variable`, which bypasses the lock. If `fn` raises, the variable is
  left unchanged.
- Variables live until `delete_variable` or `clean_namespace`. On Redis they
  are plain string keys (`{namespace}:variables:{name}`); on the filesystem,
  one file each under `variables/`.

### Heartbeats

A worker that is killed, or whose machine goes down, never calls `close()`
and stays in the registry. Heartbeats let a client tell it apart from a
worker that is busy with a long task.

While `run` or `run_forever` is running, a daemon thread writes the worker's
`time.time()` and its `heartbeat_interval` every `heartbeat_interval` seconds
(the connector's `default_heartbeat_interval`: 10 s on Redis, 30 s on the
filesystem; `Worker(..., heartbeat_interval=None)` disables it). The
thread beats even while a registered function runs for minutes, which the
main loop could not do. `close()` and a `with` block delete the heartbeat and
unregister the worker, so a clean shutdown disappears at once. The end of
`run` or `run_forever` only stops the thread: a worker interrupted with
Ctrl-C keeps its last heartbeat, goes stale, and `prune_dead_workers()` can
remove it. A dead worker leaves a stale heartbeat too.

A live worker can be pruned too: the shared drive was down for a while and
its beats failed, or the machine slept. A worker that could not beat for two
intervals publishes its queues and methods again at its next beat, because a
client may have pruned it, and it is back in the routing at once. On time, a
beat is one write to the value store and the registry is not touched.
`clean_namespace()` is different: it is an explicit reset, restart the
workers after it.

```python
client.alive_workers()                  # {'my_queue': ['redis_server:3']}, connector's default max_age
client.alive_workers(max_age=60)        # tolerate slower heartbeats
client.prune_dead_workers()             # unregister stale workers, returns their ids
```

Rules:

- A worker is dead when its heartbeat is older than its tolerance, measured
  with the reader's clock. The tolerance is `max_age` (the connector's
  `default_heartbeat_max_age` when unset: 30 s on Redis, 61 s on the
  filesystem) or three times the worker's own `heartbeat_interval`,
  whichever is longer.
  So a worker that beats every 60 s is dead after 180 s, whatever the
  reader's `max_age`. `max_age` is the floor: it absorbs a few seconds of
  clock skew and covers workers of older versions that do not publish
  their interval. The rule lives in `Connector.dead_workers`; `alive_workers`,
  `prune_dead_workers` and `gather` all use it.
- A registered worker **without** a heartbeat (older version, heartbeats
  disabled, driven with `run_once` only) counts as alive and is never pruned.
  A dead worker always has one: it wrote heartbeats while alive.
- Every registry cache refresh prunes the workers that are dead for the
  connector's default `max_age`: `update_registry_cache()`,
  `registry(update=True)`, `alive_workers(update=True)`, a cache miss in
  `cache` mode and each `gather` step. Queue selection in `always` mode
  reads the connector directly and does not prune. Call
  `prune_dead_workers(max_age)` for another threshold, or
  `update_registry_cache(prune=False)` to only read. Pruning is safe
  because each worker gets its own tolerance, because a live worker pruned
  while it could not beat comes back by itself, and because `gather` does
  not need dead entries (see below).
- Heartbeats are stored in the value store, in their own key families
  (`{namespace}:heartbeats:{worker_id}` and
  `{namespace}:heartbeat_intervals:{worker_id}` on Redis, `heartbeats_...`
  and `heartbeat_intervals_...` under `variables/` on the filesystem), so
  they never show up in `variables()`. No transport expiry is used: the
  rule is the same on every connector.

`gather` uses the same signal to recover the requests lost with a worker:

```python
fs = [client.rpc_async("price", [isin], retry=True) for isin in isins]
pending = gather(fs, timeout=600, retry_lost=True)   # [] when everything arrived
```

A worker takes a request out of the queue and then runs it. If the worker
dies while it runs the request, the request is gone. A request that is still
in the queue is not lost: a new worker will take it later, and it must not
be resent, because it would run twice. Every `step` seconds (5 by default)
`gather` checks the pending requests. It resends a request, with
`AsyncResult.retry`, when two conditions hold:

- A worker took it. Queues are FIFO, so if a request sent later to the same
  queue has been answered, the pending request is no longer in the queue.
  The answers of every client in the call count as evidence.
- No alive worker can be holding it. A worker runs one request at a time
  and takes them in FIFO order, so an alive worker that answered a request
  sent later to the same queue cannot be holding the earlier one. Each
  answer says which worker sent it. The request is lost when every alive
  registered worker on the queue answered a request sent after it; with no
  alive worker on the queue this holds by itself. Dead workers play no
  part, so a prune never hides a loss.

The request goes to a queue with alive workers that serves the method: the
same queue if it is alive again (a new worker), otherwise a random one. If
there is no such queue, `gather` logs a warning once, sets `lost=True` on
the AsyncResult and resends it as soon as a later step finds a queue. Only
requests created with `retry=True` are resent; the others get `lost=True`
and a log line. There is no limit: a resent request needs new evidence to be
resent again.

Two cases stay pending. The newest request on a queue: nothing was sent
after it, so nothing can prove that a worker took it. A queue with an alive
worker that answered nothing sent after the request (idle, slow, or serving
other clients): it may be holding it. A worker added with `register=False`
is not in the registry and cannot be excluded, so keep the functions
idempotent when you use such workers. `gather` works across several
`Client` instances with one common `timeout` and returns the AsyncResults
still pending.

### Connector contract

A connector is a subclass of `distributed_processing.Connector`. The base
class implements the naming scheme and the method registry once; a transport
only provides a few primitives. The full rules live in the docstring of
`Connector` and are checked against every implementation by
`tests/test_connector.py`. In short, a namespace holds:

- **FIFO queues** of Python objects. `enqueue(queue_ref, obj)` appends.
  `pop(queue_ref, timeout)` and `pop_multiple([queue_ref, ...], timeout)`
  return `(queue_ref, obj)` or `None` on timeout; `timeout < 0` waits
  indefinitely, `0` checks once, `> 0` waits at most that long.
  `pop_multiple` checks the queues in the given order (priority) and returns
  the first message found. `pop_all(queue_ref)` never blocks and returns
  `[obj, ...]` in FIFO order. Undecodable messages are logged and skipped.
  A responses queue has one consumer; a requests queue may have many, and
  each message reaches exactly one of them.
- **A registry** of sets: which queues serve each method, which workers listen
  on each queue. `register_methods({queue_ref: {method: fn}}, worker_id)` is
  additive and idempotent. `unregister_methods(worker_id)` removes the worker
  from every queue; a queue left without workers is dropped and removed from
  every method, and a method left without queues is dropped. It is
  coarse-grained: a method stays available while any queue serving it still
  has a worker. `methods_registry() -> {method: [queue_ref]}`,
  `workers_registry() -> {queue_ref: [worker_id]}`,
  `all_queues_for_method(method)` and `random_queue_for_method(method)`
  (`None` if no queue serves it) read it.
- **Two counters** for unique ids. `get_client_id()` / `get_server_id()`
  return `{id_prefix}_client{sep}{n}` / `{id_prefix}_server{sep}{n}`; ids are
  never reused until `clean_namespace()`, which deletes queues, registry,
  counters, variables and heartbeats.
- **Shared variables**, a key/value store of Python objects in its own key
  family. `set_variable(name, value)`, `get_variable(name, default=None)`,
  `delete_variable(name) -> bool` and `variables() -> [name, ...]` (sorted).
  Copies, last write wins, not covered by the registry lock.
  `update_variable(name, fn, default=...)` is the only read-modify-write:
  it holds `_variable_lock(key)`, one lock per variable, and returns the
  new value; `KeyError` if the variable is not set and no default is given.
- **Heartbeats**, in the value store under their own key families.
  `heartbeat(worker_id, interval=None)` stores the writer's `time.time()`
  and, if given, the interval; `heartbeats() -> {worker_id: time}`;
  `heartbeat_intervals() -> {worker_id: seconds}`;
  `dead_workers(max_age) -> {worker_id: deadline}` holds the rule (dead
  after the last beat plus `max(max_age, 3 * interval)`);
  `alive_workers(max_age) -> set`; `delete_heartbeat(worker_id) -> bool`;
  `prune_dead_workers(max_age)` unregisters and deletes the dead workers
  and returns their ids. Workers without a heartbeat key are never pruned.
  `max_age=None` means the class attribute `default_heartbeat_max_age`, and
  a `Worker` without `heartbeat_interval` uses `default_heartbeat_interval`:
  a transport overrides both (the filesystem uses 30 s and 61 s). No
  transport expiry is used. Implemented once in the base class: a new
  connector gets them from the value store primitives.
- **Names.** `get_requests_queue(name)` / `requests_queue_name(ref)` round
  trip; `get_responses_queue(client_id)`; `get_reply_to_from_id("{client_id}:{n}")`
  is the responses queue of that client.

Every public registry operation runs inside `_registry_lock()`; the
filesystem connector uses a file lock there, Redis needs none because its
commands are atomic. Queue operations are not locked: the transport itself
must hand each message to exactly one consumer.

### How to write a connector

Subclass `Connector`, set `sep` and `id_prefix`, and implement:

- `clean_namespace()`.
- `_incr(key) -> int`: atomic counter, first call returns 1.
- The set store: `_set_add(key, members)`, `_set_discard(key, members) -> int`
  (how many were present), `_set_members(key) -> set`, `_set_keys(prefix) -> list`,
  `_set_delete(key)`.
- The value store: `_value_set(key, value)`, `_value_get(key)` (raises
  `KeyError` if missing), `_value_delete(key) -> bool` (whether it existed),
  `_value_keys(prefix) -> list`. Values are Python objects: encode them with
  the connector's serializer, as the queues do.
- The queues: `enqueue`, `pop`, `pop_multiple`, `pop_all`, with the semantics above.

Override `_key(*parts)` if keys need a namespace prefix (Redis does),
`_registry_lock()` if the set store is not atomic on its own (the filesystem
does) and `_variable_lock(key)` with a real per-variable lock whenever
several processes share the transport (both do; the default is a no-op).
Instantiating a subclass that misses a primitive raises `TypeError`.
`tests/conftest.py:MemoryConnector` is the smallest complete example (about
75 lines); add the new connector to the `connector` fixture of
`tests/test_connector.py` and the contract suite becomes its acceptance test.

## Sending functions

`Client.rpc_async_fn(fn, args, kwargs)` and `rpc_sync_fn` send a local Python
function to a worker instead of calling a registered method. The function is
serialized with `dill`, base64-encoded so it travels as a plain string, and
sent as a request for the method `eval_py_function`. The worker must offer it
with `Worker.add_python_eval()`, which adds the queue `py_eval` (priority 20,
above the default 10); `add_python_eval(register=False)` keeps the queue out of
the registry, so only clients that know its name can use it.

Things to know before relying on it:

- `dill` pickles a function defined in a notebook or in `__main__` **by value**:
  it travels whole, closures included. A function imported from a module is
  pickled **by reference**: the worker must be able to import that module, at
  the same version. `examples/monte_carlo` puts the mapper in a module for
  this reason.
- Client and worker need compatible Python and `dill` versions; a mismatch
  fails at unpickling on the worker.
- Arguments and the result go through the connector's serializer (JSON on
  Redis by default), with the same limits as any other request.
- There are no batch or multi variants. To fan a function out, build the
  params with `distributed_processing.client.serialize_python_call(fn, args, kwargs)`
  and send them with `rpc_multi_async("eval_py_function", ...)`.
- It executes arbitrary code on the worker. See the security note below.

## Security note

`Worker.add_python_eval()` exposes `eval_py_function`, which deserializes with
`dill` and **executes arbitrary Python code** sent by clients (this is what
`rpc_async_fn` uses). Anyone with write access to the queues (the Redis
server or the shared directory) can execute code on the workers. Use it only
on trusted infrastructure and do not expose the transport to untrusted
networks.

## Protocol

JSON-RPC 2.0-style messages with extensions: `reply_to` (response queue),
`ack` (receipt confirmation), `is_notification`, `options` and `timing`/
`metadata` (worker, queue, execution times). Parameters travel as two keys,
`args` (list) and `kwargs` (dict), and unlike JSON-RPC 2.0 (one `params`
array *or* object) both may be present in one request, as in a Python call:
the worker runs `fn(*args, **kwargs)`, and a conflict between them (the same
parameter given twice, a missing or an unknown one) is answered with
`-32602`. Standard error codes:
`-32600` invalid request, `-32601` method not found, `-32602` invalid
params (the arguments do not fit the function signature), `-32603` internal
error (any exception raised by the function, including `TypeError`; includes
the remote traceback if the worker is created with `with_trace=True`).

## Tests and quality

```bash
pytest                       # full suite (~2 s)
pytest -m "not integration"  # unit tests only (no filesystem access)
ruff check distributed_processing tests   # lint
ruff format distributed_processing tests  # formatting
```

Unit tests use an in-memory connector (`tests/conftest.py`), with no need
for Redis or a shared directory. `tests/test_connector.py` runs the connector
contract against the memory, filesystem and Redis (fake server) connectors.
CI (GitHub Actions) runs lint + tests on Python 3.9–3.13.

## Layout

```
distributed_processing/
├── client.py                # Client: request sending, response cache
├── worker.py                # Worker: queues, dispatch and method execution
├── async_result.py          # AsyncResult and gather() (multi-client wait, retry on dead queues)
├── messages.py              # message construction and validation
├── serializers.py           # JsonSerializer (Redis default), PickleSerializer, JoblibSerializer
├── connector.py             # Connector base class: contract, naming and registry
├── redis_connector.py       # Redis transport
├── filesystem_connector.py  # filesystem transport (fs_structs)
├── exceptions.py            # RemoteException
└── utils.py                 # fsworker/fsclient helpers; node/fsnode/redisnode (worker subprocesses)
```

`examples/` contains usage notebooks (filesystem, Redis, and a Monte Carlo
pricing example for autocallables).

Notebooks are committed **without outputs**: saved outputs carry tracebacks
with local paths and user names, and they bloat diffs. Before committing one,
strip them — `pip install nbstripout && nbstripout --install` sets up the git
filter declared in `.gitattributes` and does it automatically.
