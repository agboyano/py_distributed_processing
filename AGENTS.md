# AGENTS.md

Guidance for coding agents (and humans) working on `distributed_processing`.

## What this library is

Lightweight distributed computing on top of message queues, with an RPC
protocol in the style of JSON-RPC 2.0. A **worker** registers functions and
listens on request queues; a **client** sends requests and collects the
responses, synchronously or through `AsyncResult`. The transport is a
pluggable **connector**: Redis or a shared directory (`fs_structs`).

- `distributed_processing/client.py`: `Client` (queue selection, request
  sending, response cache, shared variables, `alive_workers` /
  `prune_dead_workers`), `serialize_python_call`.
- `distributed_processing/worker.py`: `Worker` (queues with priorities,
  dispatch, `check_params`, `run` / `run_forever`, heartbeat thread),
  `eval_py_function`.
- `distributed_processing/async_result.py`: `AsyncResult`, `gather` (waits
  for AsyncResults of several clients, returns the pending ones,
  `retry_lost` resends what a dead worker took).
- `distributed_processing/messages.py`: message construction and predicates.
- `distributed_processing/connector.py`: `Connector` base class. Its docstring
  is the transport contract: names, queues, registry, variables, heartbeats,
  atomicity.
- `distributed_processing/redis_connector.py`, `filesystem_connector.py`: the
  two transports. `serializers.py`: `JsonSerializer` (Redis default),
  `PickleSerializer`, `JoblibSerializer`. `utils.py`: `fsclient`, `fsworker`,
  `fsnode` helpers for the filesystem transport.
- `tests/`: pytest. Unit tests run on `tests/conftest.py:MemoryConnector`, the
  smallest complete connector. `tests/test_connector.py` is the contract
  suite and runs against memory, filesystem (marker `integration`) and a fake
  Redis (`tests/test_redis_connector.py:FakeRedis`).
- `examples/`: `redis/` and `filesystem/` client and worker notebooks are the
  reference of use and are kept in parallel (a cell added to one goes to the
  other); `monte_carlo/` is a pricing example.
- `.github/workflows/ci.yml`: ruff, then `pytest -m "not integration"` on
  Python 3.9 to 3.13. Integration tests need `fs_structs`, an unpublished
  sibling project, so they only run locally.

README.md documents the protocol, the connector contract and the shared
variables. Read it first.

## Contracts you must not break

- **Connector contract.** The rules live in the `Connector` docstring and
  `tests/test_connector.py` checks them against every connector. A new
  primitive is implemented in `RedisConnector`, `FileSystemConnector` and
  `MemoryConnector`, plus `FakeRedis` when it needs a Redis command, and it
  gets a contract test. A new connector is added to the `connector` fixture
  of `test_connector.py`; the suite is its acceptance test.
- **Encoding.** Queues and variables carry Python objects; the connector owns
  the wire format (JSON by default on Redis, an `fs_structs` serializer on the
  filesystem). Every client and worker of a namespace uses the same one.
  `serialize_python_call` base64-encodes the dill payload unconditionally, so
  `eval_py_function` has a single wire format on every transport.
- **Queue selection, three rules.** (1) An explicit `queue` is used as is, in
  every `check_registry` mode, for single and batch requests: the registry is
  not consulted (queues added with `register=False` are reachable) and the
  worker answers -32601 for methods it lacks. (2) No queue and `"never"`: the
  request goes to `default_queue`. (3) No queue and `"cache"` or `"always"`:
  the registry decides; `ValueError` if no queue serves the method (all the
  methods, for a batch). `default_queue` is never a fallback there.
  `check_registry` is a property: values are normalized (case, spaces),
  anything outside `CHECK_REGISTRY_MODES` raises `ValueError`, and setting
  `"cache"` fills an empty registry cache.
- **Parameters.** A request may carry both `args` and `kwargs`. This is a
  deliberate departure from JSON-RPC 2.0 (one `params` array or object): the
  worker runs `fn(*args, **kwargs)` and `check_params` turns a clash into
  -32602. Error codes: -32600 invalid request, -32601 method not found,
  -32602 invalid params (arguments do not fit the signature), -32603 any
  exception raised inside the function.
- **Shared variables.** `get_variable` returns a copy (the value goes through
  the serializer). `set_variable` takes no lock on purpose: one atomic write,
  last write wins, one writer and many readers is the expected use.
  `update_variable(name, fn, default=MISSING)` is the only read-modify-write:
  it holds `_variable_lock(key)` (a lock directory per variable on the
  filesystem, a redis-py `Lock` on Redis) and raises `KeyError` when the
  variable is missing and no default is given (`None` is a valid default).
  A variable updated with `update_variable` is not written with
  `set_variable`, which bypasses the lock.
- **Heartbeats.** Stored in the value store under the `heartbeats` key
  family, implemented once in `Connector` over the value primitives: no
  transport expiry (`EXPIRE`), the reader compares the stored `time.time()`
  with its own clock and `max_age` (default 30 s, three times the worker's
  default interval). The worker beats from a daemon thread started by `run`
  / `run_forever` and stopped in their `finally` and in `close()`; `run_once`
  alone never beats. A registered worker without a heartbeat key counts as
  alive and is never pruned (compatibility). Pruning is explicit
  (`prune_dead_workers`): nothing in queue selection or in the worker does it.
- **gather.** `gather(fs, timeout, step, retry_lost, max_age) -> list` of the
  AsyncResults still pending (`[]` = all arrived); `fs` may mix clients and
  `timeout` is one common deadline. Clients are waited sequentially, no
  threads: with a common deadline the outcome is the same. `step` is the
  polling period of the lost-request check, not a wait. `retry_lost` resends
  only a request it can prove a dead worker took, never one still in the
  queue. Taken: FIFO evidence, `Client.pending[id]` (last send time, refreshed
  by `retry`; `creation_time` is not) compared with `metadata.timing.request_sent`
  of every answered result in `fs` on the same queue ref, whatever its client
  (same process, same clock). Died: a registered worker with an expired
  heartbeat whose last beat plus `max_age` is after the send time, or no
  alive worker at all. Target: an alive queue serving the method, the same
  one first; none: warn once, flag `AsyncResult.lost` (survives pruning) and
  resend on a later step. Only `retry=True` requests are resent; no cap,
  each resend needs fresh evidence. Not decidable: the newest request on a
  queue (nothing behind it, stays pending; a probe request was tried and
  rejected as too convoluted) and several alive workers plus a dead one on a
  queue (a long request may be resent). An earlier FIFO heuristic without
  the dead-worker and heartbeat-bound conditions was removed for misfiring
  with several workers. Implementation notes and docs are written in plain
  English for non-native readers: short sentences, step by step.
- **Ids.** Request ids are `{client_id}:{n}`; a client id may contain `:`, so
  the responses queue is derived by splitting on the last one.
  `clean_namespace` resets the counters.
- **Worker.** A registered function does not receive the worker: it reaches
  shared variables or `stop()` through a closure. `add_python_eval` executes
  arbitrary code sent by clients: trusted infrastructure only, and say so
  wherever it appears. `run` is for notebooks (errors stop the cell),
  `run_forever` for services (errors are logged and retried with backoff).
  Both start the heartbeat thread; a transport error inside it is logged,
  never raised.
- **Compatibility.** Other projects call the public API with positional
  arguments. New parameters go last, with a default that keeps the current
  behaviour (`rpc_batch_sync` got `queue` after `timeout` for this reason).
  Do not rename or remove public names.
- **Python 3.9.** `from __future__ import annotations` is used, so `X | Y` is
  fine in annotations but not at runtime; no `match`. Dependencies: `dill` in
  the core; `redis`, `fs_structs` and `watchdog` only as extras.
- **Version.** `__version__` in `distributed_processing/__init__.py` and
  `version` in `pyproject.toml` stay in sync. Bump on API additions.

## Documentation conventions

- Google-style docstrings: `Args:`, `Returns:`, `Raises:`, and `Examples:`
  when a short example helps. Doctests are not run in this repository.
- Implementation details (why an encoding, why a lock is or is not taken,
  platform behaviour) go in an `# Implementation notes.` comment block right
  before the `def`, not in the docstring. The docstring is for the user; the
  comment block is for the maintainer.
- Write for readers whose first language is not English: short sentences,
  one idea per sentence, no idioms.
- When a contract changes, update the `Connector` docstring **and** the
  README sections *Connector contract*, *How to write a connector*, *Shared
  variables* or *Protocol*, whichever applies. The `Client` class docstring
  states the queue selection rules; every `queue` parameter repeats the same
  wording.
- Notebooks are a reference of use: one to four comment lines per cell
  saying what the cell shows and what to expect, no test frameworks, no
  helper code that belongs in the library. They are committed **without
  outputs** (outputs carry tracebacks with local paths and user names):
  `.gitattributes` declares the `nbstripout` filter, run
  `nbstripout --install` once per clone, or clear the outputs with
  `nbformat` before committing.
- This is a public repository: nothing about a company, a team or a person;
  no internal paths or server names. The `.env` files under `examples/` are
  local and ignored by git.

## Coding conventions

- Do not delete code on your own initiative. Code that becomes unused is left
  in place, marked as such, and the decision is reported to the owner.
- Keep it simple: prefer an existing library or a small helper over a new
  abstraction; prefer the standard library.
- New behaviour comes with a test: a contract test in `test_connector.py` if
  it is connector behaviour, otherwise in `test_client.py` or
  `test_worker.py`. The in-memory connector and the fake Redis must keep
  supporting whatever the real connectors do.
- `ruff check` and `ruff format --check` stay clean (`pyproject.toml`).

## How to verify

```
pip install -e ".[dev]"                     # + fs_structs from the sibling repo for integration tests
pytest -q                                   # full suite, ~10 s
pytest -m "not integration"                 # what CI runs
ruff check distributed_processing tests
ruff format --check distributed_processing tests
python -c "import nbformat; nbformat.validate(nbformat.read('examples/redis/redis_client.ipynb', 4))"
```

`FakeRedis` covers the connector logic, not the server: it does not run
`BLPOP` blocking or `redis.lock.Lock`. A change in a connector gets one manual
run of the example notebooks against a live Redis and a shared directory
before it is reported as done.

## Reporting

Report what was verified and where (unit suite, integration suite, live
Redis, notebooks), and say plainly what was not verified. A non-zero test
run is never described as success.
