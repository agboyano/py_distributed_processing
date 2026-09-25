"""Nodes: a master Worker that starts, lists and kills worker subprocesses.

`node` turns any `Worker`, on any connector, into a node whose methods
(`create_worker`, `list_processes`, `kill_process`...) are callable via
RPC. The one-line constructors per transport, `fsnode` and `redisnode`,
live in `utils`.
"""

from __future__ import annotations

import atexit
import base64
import importlib
import logging
import multiprocessing as mp
import os
import threading
import traceback
from collections import namedtuple
from time import time
from typing import Any, Callable

import dill

from .worker import Worker

# Serialize the whole closure of the functions sent between processes.
dill.settings["recurse"] = True

logger = logging.getLogger(__name__)

# Worker subprocesses always start with `spawn`, on every platform. The
# master runs threads (heartbeat, watchdog observers on the filesystem) and
# holds connections; forking it (the Linux default before 3.14) can leave a
# copied lock held forever in the child. `spawn` starts a clean interpreter,
# which is also what Windows and Jupyter give us anyway: the design below
# assumes it.
_SPAWN = mp.get_context("spawn")

WorkerProcess = namedtuple("WorkerProcess", ["p", "pid", "worker_type", "worker_id"])


def serialize(x: Any) -> bytes:
    "Serializes any python object with dill."
    return dill.dumps(x)


def deserialize(x: bytes) -> Any:
    "Deserializes a dill-serialized python object."
    return dill.loads(x)


# Implementation notes.
#
# Why the subprocess machinery looks like this. The same code has to work
# as a script and in a Jupyter notebook on Windows, where `multiprocessing`
# uses `spawn`: the child is a fresh interpreter that unpickles the
# `Process` target and its arguments. A function defined in a notebook
# lives in a `__main__` that has no file, so the child cannot import it.
# Hence:
#
# - The target, `_create_worker`, is a function of this module: importable
#   anywhere.
# - The user's constructor (and its args and kwargs) travel as one dill
#   blob. dill pickles a notebook function by value, with the globals it
#   refers to (`recurse`), and the child rebuilds it. A constructor from an
#   importable module is pickled by reference.
# - The constructor builds the worker **and its connector** inside the
#   child. Connections, watchdog observers and threads are never shared
#   with the parent.
# - The child reports back through a one-way `Pipe`: `("ok", worker_id)`
#   once the worker exists, or `("error", traceback)` if the constructor
#   raised. The parent polls that pipe; a child that dies before reporting
#   closes its end and the parent sees `EOFError` at once. No `Manager`
#   process, no shared dict, no polling loop.
#
# Orphan guard. `atexit` handlers of the master do not run after a hard
# kill (`taskkill /F`, SIGKILL), so the children would keep serving
# requests as orphans, registered and beating. Each child runs a daemon
# thread that waits on `multiprocessing.parent_process()` (the parent's
# sentinel); when the parent is gone it closes the worker (unregister,
# delete heartbeat) and exits with `os._exit`, because the main thread is
# blocked in the worker loop.
def _watch_parent(worker: Worker) -> None:
    "Starts a daemon thread that closes `worker` and exits when the parent dies."
    parent = mp.parent_process()
    if parent is None:
        return

    def guard():
        parent.join()
        logger.warning(
            f"Worker {worker.worker_id}: the parent process is gone, exiting."
        )
        try:
            worker.close()
        except Exception:
            pass
        os._exit(0)

    threading.Thread(target=guard, name="parent-guard", daemon=True).start()


# Implementation notes.
#
# An import path is resolved here, in the child, never in the node. The
# child is a fresh interpreter, so the import always reads the current
# file on disk: a new module, or a new version of one, is picked up by the
# next `create_worker` without restarting the node and without any reload
# logic. The node process itself never imports user modules.
def _import_constructor(path: str) -> Callable:
    """Returns the callable named by an import path `package.module:attr`."""
    module_name, sep, attr = path.rpartition(":")
    if not sep or not module_name or not attr:
        raise ValueError(
            f"Invalid worker constructor path {path!r}: expected 'package.module:attr'."
        )
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attr)
    except AttributeError:
        raise ValueError(f"Module {module_name!r} has no attribute {attr!r}.") from None


def _create_worker(payload: bytes, conn) -> None:
    """Target of the worker subprocesses. Builds the worker, reports and runs it.

    Args:
        payload (bytes): dill blob of `(constructor, args, kwargs)`, where
            `constructor` is a callable or an import path
            `package.module:attr` that this process imports.
        conn: Sending end of the pipe to the parent.

    """
    try:
        constructor, args, kwargs = deserialize(payload)
        if isinstance(constructor, str):
            constructor = _import_constructor(constructor)
        worker = constructor(*args, **kwargs)
    except BaseException:
        conn.send(("error", traceback.format_exc()))
        conn.close()
        return
    conn.send(("ok", worker.worker_id))
    conn.close()
    _watch_parent(worker)
    # These subprocesses are unattended workers in a real deployment, so use
    # `run_forever`: connector errors (shared drive not responding, lock
    # timeout, corrupt message...) are logged and retried with backoff instead
    # of killing the subprocess. `run` is meant for development (notebooks,
    # tests), where an exception stopping the loop with a traceback is useful.
    worker.run_forever()


def _wait_report(conn, p, timeout: float) -> tuple:
    """Waits for the child's report. Returns ("ok", worker_id) or ("error", reason)."""
    deadline = time() + timeout
    while True:
        left = deadline - time()
        if left <= 0:
            return "error", f"no report from the process within {timeout} s"
        if conn.poll(min(0.5, left)):
            try:
                return conn.recv()
            except EOFError:
                p.join(timeout=5)
                return (
                    "error",
                    f"the process exited with code {p.exitcode} before reporting",
                )
        if not p.is_alive():
            p.join(timeout=5)
            return (
                "error",
                f"the process exited with code {p.exitcode} before reporting",
            )


def node(
    master: Worker,
    workers_constructors: dict | None = None,
    creation_processes_timeout: float = 60,
    allow_remote_constructors: bool = False,
) -> Worker:
    """Turns `master` into a node: a Worker that manages workers in subprocesses.

    Works with any connector. The master listens on a queue named after
    its `worker_id` and exposes these methods, callable remotely via RPC
    (or locally with `master.exec_method(name, args, queue=master.worker_id)`):

    - `create_worker(worker_type, args=None, kwargs=None)`: starts a new
      subprocess running `constructor(*args, **kwargs).run_forever()` and
      returns `(pid, worker_type, worker_id)`. `worker_type` is a key of
      `workers_constructors` or an import path `package.module:attr`,
      which the **subprocess** imports: a module on the node's path can
      provide new types, or new versions of a type, without restarting
      the node. Raises `ValueError` for an unknown type and `RuntimeError`
      if the worker did not start: the constructor raised or could not be
      imported (the message carries the remote traceback) or it did not
      report within `creation_processes_timeout`. A remote caller gets a
      `RemoteException`.
    - `worker_types()`: sorted names of `workers_constructors`, including
      the ones added with `create_worker_fn`.
    - `create_worker_fn(str_fn, args=None, kwargs=None, name=None)`: only
      if `allow_remote_constructors` is True. Starts a worker from a
      constructor sent by the client, `str_fn` being the base64-encoded
      dill payload that `Client.serialize_python_call` builds, so from a
      client it is
      `client.rpc_sync("create_worker_fn", serialize_python_call(fn, args, kwargs) + [name], queue=node_id)`.
      With `name`, the constructor is also added to `workers_constructors`
      for later `create_worker(name)` calls. Returns
      `(pid, name or fn.__name__, worker_id)`. The globals the constructor
      uses travel with it (`serialize_python_call` pickles with
      `recurse=True`); modules and classes must be importable on the node.
      Passing the settings (host, namespace, path...) as arguments keeps
      the constructor reusable. SECURITY WARNING: this executes
      arbitrary Python code sent by clients, like `eval_py_function`.
      Only enable it on trusted infrastructure.
    - `list_processes()`: `[(pid, worker_type, worker_id), ...]` of the
      alive worker subprocesses.
    - `kill_process(pid) -> bool`: terminates that worker subprocess,
      unregisters it and deletes its heartbeat. False if no alive worker
      has that pid.
    - `kill_processes(pids) -> list[bool]`: one result per pid, in order.
    - `kill_all_processes() -> list[int]`: pids of the workers killed.
    - `cleanup()`: kills every worker and closes the master (unregister,
      delete heartbeat). Also registered with `atexit`.

    A worker subprocess that dies on its own is noticed by the next call
    to any of these methods: it is unregistered and its heartbeat deleted.
    When the master itself is killed hard (`taskkill /F`, SIGKILL) the
    workers close themselves and exit within seconds.

    Subprocesses are started with the `spawn` method on every platform.
    Each constructor is dill-serialized into its subprocess, so functions
    defined in a notebook or in `__main__` work; the constructor must build
    the worker **and its connector** itself, never reuse the master's. The
    subprocesses do not inherit the logging configuration.

    Several nodes may publish the same methods on one namespace: a client
    that does not pass `queue` reaches a random one. Target a node with
    `client.rpc_sync("create_worker", ["w"], queue=<its worker_id>)`.

    The caller must call `run()` (or `run_forever()`) on the returned
    master Worker to start serving.

    Args:
        master (Worker): The Worker that will serve the methods above.
            It must not have been run yet. A meaningful `worker_id` makes
            the queue easy to target.
        workers_constructors (dict, optional): {worker_type: constructor}
            with the constructors (callables returning a Worker) that
            `create_worker` can launch. Defaults to None ({}).
        creation_processes_timeout (float): Maximum seconds to wait for a
            new worker subprocess to report that it started. Defaults
            to 60.
        allow_remote_constructors (bool): If True, register
            `create_worker_fn` (see the SECURITY WARNING above). Defaults
            to False.

    Returns:
        Worker: `master`, with the methods above registered and published.

    """
    workers_constructors = {} if workers_constructors is None else workers_constructors
    connector = master.connector
    processes: list = []

    def _forget(wp: WorkerProcess) -> None:
        "Removes a dead worker from the registry. Best effort."
        try:
            connector.unregister_methods(wp.worker_id)
            connector.delete_heartbeat(wp.worker_id)
        except Exception:
            logger.exception(
                f"Node {master.worker_id}: could not unregister worker {wp.worker_id}."
            )

    def _reap() -> None:
        "Forgets the workers whose process is not alive any more."
        for wp in [wp for wp in processes if not wp.p.is_alive()]:
            processes.remove(wp)
            wp.p.join(timeout=1)
            logger.warning(
                f"Node {master.worker_id}: worker {wp.worker_id} ({wp.worker_type}, pid {wp.pid}) "
                f"exited on its own with code {wp.p.exitcode}. Unregistered."
            )
            _forget(wp)

    def _terminate(p) -> None:
        p.terminate()
        p.join(timeout=30)
        if p.is_alive():
            logger.warning(
                f"Process {p.pid} did not stop after terminate(). Killing it."
            )
            p.kill()
            p.join(timeout=30)

    def _kill_process(wp: WorkerProcess) -> bool:
        _terminate(wp.p)
        processes.remove(wp)
        _forget(wp)
        logger.info(
            f"Node {master.worker_id}: killed worker {wp.worker_id} ({wp.worker_type}, pid {wp.pid})."
        )
        return True

    def list_processes() -> list:
        _reap()
        return [(wp.pid, wp.worker_type, wp.worker_id) for wp in processes]

    def kill_process(pid: int) -> bool:
        _reap()
        for wp in processes:
            if wp.pid == pid:
                return _kill_process(wp)
        return False

    def kill_processes(pids: list) -> list:
        return [kill_process(pid) for pid in pids]

    def kill_all_processes() -> list:
        _reap()
        return [wp.pid for wp in list(processes) if _kill_process(wp)]

    def _spawn(constructor, worker_type: str, args, kwargs) -> tuple:
        "Starts a subprocess for `constructor` (a callable or an import path)."
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        payload = serialize((constructor, args, kwargs))

        parent_conn, child_conn = _SPAWN.Pipe(duplex=False)
        p = _SPAWN.Process(
            target=_create_worker,
            args=(payload, child_conn),
            name=f"worker-{worker_type}",
        )
        p.start()
        child_conn.close()  # only the child keeps the sending end
        status, detail = _wait_report(parent_conn, p, creation_processes_timeout)
        parent_conn.close()

        if status != "ok":
            if p.is_alive():
                _terminate(p)
            raise RuntimeError(
                f"Worker of type {worker_type!r} did not start: {detail}"
            )

        wp = WorkerProcess(p, p.pid, worker_type, detail)
        processes.append(wp)
        logger.info(
            f"Node {master.worker_id}: started worker {wp.worker_id} ({worker_type}, pid {wp.pid})."
        )
        return p.pid, worker_type, wp.worker_id

    def create_worker(
        worker_type: str, args: list | None = None, kwargs: dict | None = None
    ) -> tuple:
        _reap()
        if worker_type in workers_constructors:
            constructor = workers_constructors[worker_type]
        elif ":" in worker_type:
            constructor = worker_type  # an import path, resolved by the subprocess
        else:
            raise ValueError(
                f"Unknown worker type {worker_type!r}. Known types: "
                f"{sorted(workers_constructors)}; an import path "
                "'package.module:attr' is also accepted."
            )
        return _spawn(constructor, worker_type, args, kwargs)

    def worker_types() -> list:
        return sorted(workers_constructors)

    # Implementation notes.
    #
    # The payload is unpickled here, in the node, and not passed on as
    # bytes, because a named constructor is kept in `workers_constructors`
    # for later `create_worker` calls. Unpickling does not run the
    # constructor; the trust decision is `allow_remote_constructors`.
    def create_worker_fn(
        str_fn: str,
        args: list | None = None,
        kwargs: dict | None = None,
        name: str | None = None,
    ) -> tuple:
        _reap()
        constructor = deserialize(base64.b64decode(str_fn))
        if name is not None:
            workers_constructors[name] = constructor
        worker_type = (
            name if name is not None else getattr(constructor, "__name__", "remote")
        )
        return _spawn(constructor, worker_type, args, kwargs)

    def cleanup() -> list:
        killed = kill_all_processes()
        master.close()
        return killed

    master_funcs: dict[str, Callable] = {
        "create_worker": create_worker,
        "worker_types": worker_types,
        "list_processes": list_processes,
        "kill_process": kill_process,
        "kill_processes": kill_processes,
        "kill_all_processes": kill_all_processes,
        "cleanup": cleanup,
    }
    if allow_remote_constructors:
        master_funcs["create_worker_fn"] = create_worker_fn
        logger.warning(
            f"Node {master.worker_id}: create_worker_fn is enabled. It executes "
            "code sent by clients; trusted infrastructure only."
        )

    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()
    atexit.register(cleanup)

    return master
