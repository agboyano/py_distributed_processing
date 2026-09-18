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
  (`check_registry="cache" | "always" | other`).
- Queues with priorities; queues of equal priority are shuffled on each
  iteration.
- Notifications (requests without a response), optional acks and retries
  (`AsyncResult.retry`, `gather`).
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
worker is removed from the registry on shutdown.

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

To launch a node with several workers in remotely managed subprocesses
(create/list/kill workers via RPC), see `distributed_processing.utils.fsnode`.

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
  counters and variables.
- **Shared variables**, a key/value store of Python objects in its own key
  family. `set_variable(name, value)`, `get_variable(name, default=None)`,
  `delete_variable(name) -> bool` and `variables() -> [name, ...]` (sorted).
  Copies, last write wins, not covered by the registry lock.
  `update_variable(name, fn, default=...)` is the only read-modify-write:
  it holds `_variable_lock(key)`, one lock per variable, and returns the
  new value; `KeyError` if the variable is not set and no default is given.
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
`metadata` (worker, queue, execution times). Standard error codes:
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
├── async_result.py          # AsyncResult and gather()
├── messages.py              # message construction and validation
├── serializers.py           # JsonSerializer (Redis default), PickleSerializer, JoblibSerializer
├── connector.py             # Connector base class: contract, naming and registry
├── redis_connector.py       # Redis transport
├── filesystem_connector.py  # filesystem transport (fs_structs)
├── exceptions.py            # RemoteException
└── utils.py                 # fsworker/fsclient/fsnode (filesystem helpers)
```

`examples/` contains usage notebooks (filesystem, Redis, and a Monte Carlo
pricing example for autocallables).

Notebooks are committed **without outputs**: saved outputs carry tracebacks
with local paths and user names, and they bloat diffs. Before committing one,
strip them — `pip install nbstripout && nbstripout --install` sets up the git
filter declared in `.gitattributes` and does it automatically.
