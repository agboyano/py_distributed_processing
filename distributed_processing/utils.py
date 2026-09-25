"""Helpers: one-line constructors and *nodes* that manage worker subprocesses.

`fsclient` / `fsworker` build a `Client` / `Worker` on the filesystem
transport. `node` turns any `Worker` into a node that starts, lists and
kills worker subprocesses on request; `fsnode` and `redisnode` build the
node on the filesystem and on Redis. The connectors are imported lazily,
so this module can be imported with only one of the optional extras
installed.
"""

from __future__ import annotations

import atexit
import logging
import multiprocessing as mp
import os
import threading
import traceback
from collections import namedtuple
from time import time
from typing import Any, Callable

import dill

from .client import Client
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


def fsclient(
    NS_PATH: str,
    check_registry: str = "cache",
    with_watchdog: bool = True,
    pop_watchdog_timeout: float = 10,
) -> Client:
    """Builds a Client on a filesystem namespace.

    Convenience constructor for `Client(FileSystemConnector(NS_PATH))`.

    Args:
        NS_PATH (str): Directory shared by clients and workers.
        check_registry (str): Queue selection mode ('cache', 'always' or
            'never', see `Client`). Defaults to 'cache'.
        with_watchdog (bool): If True, blocking pops wait for filesystem
            events; if False, they poll. Defaults to True.
        pop_watchdog_timeout (float): Seconds to wait for a file event
            before re-checking the queue. Defaults to 10.

    Returns:
        Client

    """
    from .filesystem_connector import FileSystemConnector

    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = with_watchdog
    fs_connector.pop_watchdog_timeout = pop_watchdog_timeout
    return Client(fs_connector, check_registry=check_registry)


def fsworker(
    NS_PATH: str,
    clean: bool = False,
    with_watchdog: bool = True,
    worker_id: str | None = None,
    watchdog_timeout: float = 60,
) -> Worker:
    """Builds a Worker on a filesystem namespace.

    Convenience constructor for `Worker(FileSystemConnector(NS_PATH))`.
    Remember to call `add_requests_queue`, `update_methods_registry` and
    `run` on the returned Worker.

    Args:
        NS_PATH (str): Directory shared by clients and workers.
        clean (bool): If True, wipe the namespace (queues and registry)
            before starting. Defaults to False.
        with_watchdog (bool): If True, blocking pops wait for filesystem
            events; if False, they poll. Defaults to True.
        worker_id (str, optional): Worker identifier. Defaults to None
            (a new one is generated).
        watchdog_timeout (float): Seconds to wait for a file event before
            re-checking the queues. Defaults to 60.

    Returns:
        Worker

    """
    from .filesystem_connector import FileSystemConnector

    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = with_watchdog
    fs_connector.pop_watchdog_timeout = watchdog_timeout
    if clean:
        fs_connector.clean_namespace()

    return Worker(fs_connector, worker_id=worker_id)


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


def _create_worker(payload: bytes, conn) -> None:
    """Target of the worker subprocesses. Builds the worker, reports and runs it.

    Args:
        payload (bytes): dill blob of `(constructor, args, kwargs)`.
        conn: Sending end of the pipe to the parent.

    """
    try:
        constructor, args, kwargs = deserialize(payload)
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
) -> Worker:
    """Turns `master` into a node: a Worker that manages workers in subprocesses.

    Works with any connector. The master listens on a queue named after
    its `worker_id` and exposes these methods, callable remotely via RPC
    (or locally with `master.exec_method(name, args, queue=master.worker_id)`):

    - `create_worker(worker_type, args=None, kwargs=None)`: starts a new
      subprocess running
      `workers_constructors[worker_type](*args, **kwargs).run_forever()`
      and returns `(pid, worker_type, worker_id)`. Raises `ValueError` for
      an unknown type and `RuntimeError` if the worker did not start: the
      constructor raised (the message carries the remote traceback) or it
      did not report within `creation_processes_timeout`. A remote caller
      gets a `RemoteException`.
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

    def create_worker(
        worker_type: str, args: list | None = None, kwargs: dict | None = None
    ) -> tuple:
        _reap()
        if worker_type not in workers_constructors:
            raise ValueError(
                f"Unknown worker type {worker_type!r}. Known types: {sorted(workers_constructors)}"
            )
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        payload = serialize((workers_constructors[worker_type], args, kwargs))

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

    def cleanup() -> list:
        killed = kill_all_processes()
        master.close()
        return killed

    master_funcs: dict[str, Callable] = {
        "create_worker": create_worker,
        "list_processes": list_processes,
        "kill_process": kill_process,
        "kill_processes": kill_processes,
        "kill_all_processes": kill_all_processes,
        "cleanup": cleanup,
    }

    master.add_requests_queue(master.worker_id, master_funcs)
    master.update_methods_registry()
    atexit.register(cleanup)

    return master


def fsnode(
    NS_PATH: str,
    clean: bool = False,
    with_watchdog: bool = True,
    worker_id: str | None = None,
    workers_constructors: dict | None = None,
    watchdog_timeout: float = 60,
    creation_processes_timeout: float = 60,
) -> Worker:
    """Builds a node on a filesystem namespace. See `node`.

    Equivalent to `node(fsworker(NS_PATH, ...), workers_constructors,
    creation_processes_timeout)`. The caller must call `run()` on the
    returned master Worker to start serving.

    Args:
        NS_PATH (str): Directory shared by clients and workers.
        clean (bool): If True, wipe the namespace (queues and registry)
            before starting. Defaults to False.
        with_watchdog (bool): If True, blocking pops wait for filesystem
            events; if False, they poll. Defaults to True.
        worker_id (str, optional): Master worker identifier (and simple
            name of its requests queue). Defaults to None (a new one is
            generated).
        workers_constructors (dict, optional): {worker_type: constructor}
            with the constructors (callables returning a Worker) that
            `create_worker` can launch. They are dill-serialized into the
            subprocesses. Defaults to None ({}).
        watchdog_timeout (float): Seconds to wait for a file event before
            re-checking the queues. Defaults to 60.
        creation_processes_timeout (float): Maximum seconds to wait for a
            new worker subprocess to report that it started. Defaults
            to 60.

    Returns:
        Worker: The master Worker, already registered. Call its `run`
            method to start serving.

    """
    master = fsworker(
        NS_PATH,
        clean=clean,
        with_watchdog=with_watchdog,
        worker_id=worker_id,
        watchdog_timeout=watchdog_timeout,
    )
    return node(master, workers_constructors, creation_processes_timeout)


def redisnode(
    redis_host: str = "localhost",
    redis_port: int = 6379,
    redis_db: int = 0,
    namespace: str = "tasks",
    serializer=None,
    clean: bool = False,
    worker_id: str | None = None,
    workers_constructors: dict | None = None,
    creation_processes_timeout: float = 60,
) -> Worker:
    """Builds a node on a Redis namespace. See `node`.

    Equivalent to `node(Worker(RedisConnector(...), worker_id=worker_id),
    workers_constructors, creation_processes_timeout)`. The caller must
    call `run()` on the returned master Worker to start serving. Give the
    node an explicit `worker_id`: it is the queue name a client targets
    with `queue=`, and a generated Redis id contains ':'.

    Args:
        redis_host (str): Redis server host. Defaults to 'localhost'.
        redis_port (int): Redis server port. Defaults to 6379.
        redis_db (int): Redis database number. Defaults to 0.
        namespace (str): Prefix for every key. Defaults to 'tasks'.
        serializer (optional): See `RedisConnector`. Defaults to None
            (`JsonSerializer()`).
        clean (bool): If True, delete every key of the namespace before
            starting, including the registry of every other worker and
            node that shares it. Defaults to False.
        worker_id (str, optional): Master worker identifier (and simple
            name of its requests queue). Defaults to None (a new one is
            generated).
        workers_constructors (dict, optional): {worker_type: constructor}
            with the constructors (callables returning a Worker) that
            `create_worker` can launch. Each constructor must build its
            own `RedisConnector`. Defaults to None ({}).
        creation_processes_timeout (float): Maximum seconds to wait for a
            new worker subprocess to report that it started. Defaults
            to 60.

    Returns:
        Worker: The master Worker, already registered. Call its `run`
            method to start serving.

    """
    from .redis_connector import RedisConnector

    connector = RedisConnector(redis_host, redis_port, redis_db, namespace, serializer)
    if clean:
        connector.clean_namespace()
    master = Worker(connector, worker_id=worker_id)
    return node(master, workers_constructors, creation_processes_timeout)
