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
  default. The `pickle`, `dill` and `msgpack` modules work as they are
  (`RedisConnector("localhost", serializer=pickle)`). The connection uses
  `decode_responses=False`, so binary formats are safe; a message that cannot
  be decoded is logged and skipped.
- `FileSystemConnector(base_path, temp_dir=None, serializer=...)`: `serializer`
  is an `fs_structs` serializer (`joblib_serializer` by default, also
  `pickle_serializer` and `json_serializer`), the same one used for the
  registry.

Every client and worker on a namespace must use the same serializer.

### Connector contract

A connector is any object with these methods (duck typing, no base class);
`tests/conftest.py:MemoryConnector` is the smallest complete example.

- Names: `get_requests_queue(name) -> ref`, `requests_queue_name(ref) -> name`,
  `get_responses_queue(client_id) -> ref`, `get_reply_to_from_id(request_id) -> ref`,
  `get_client_id()`, `get_server_id()`.
- Registry: `register_methods({queue_ref: {method: fn}}, worker_id)`,
  `unregister_methods(worker_id)`, `methods_registry() -> {method: [queue_ref]}`,
  `workers_registry() -> {queue_ref: [worker_id]}`, `all_queues_for_method(method)`,
  `random_queue_for_method(method)`.
- Queues, carrying Python objects: `enqueue(queue_ref, obj)`,
  `pop(queue_ref, timeout) -> (queue_ref, obj) | None`,
  `pop_multiple([queue_ref, ...], timeout) -> (queue_ref, obj) | None` (queues in
  priority order), `pop_all(queue_ref) -> [obj, ...]`. `timeout <= 0` waits
  indefinitely in `pop`/`pop_multiple`.

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
for Redis or a shared directory. CI (GitHub Actions) runs lint + tests on
Python 3.9–3.13.

## Layout

```
distributed_processing/
├── client.py                # Client: request sending, response cache
├── worker.py                # Worker: queues, dispatch and method execution
├── async_result.py          # AsyncResult and gather()
├── messages.py              # message construction and validation
├── serializers.py           # JsonSerializer (RedisConnector default)
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
