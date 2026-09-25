"""One-line constructors per transport.

`fsclient` / `fsworker` build a `Client` / `Worker` on the filesystem
transport; `fsnode` and `redisnode` build a node (see `node.node`) on the
filesystem and on Redis. The connectors are imported lazily, so this
module can be imported with only one of the optional extras installed.
`node`, `serialize` and `deserialize` are re-exported from `node` for
compatibility.
"""

from __future__ import annotations

from .client import Client
from .node import deserialize, node, serialize  # noqa: F401
from .worker import Worker


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


def fsnode(
    NS_PATH: str,
    clean: bool = False,
    with_watchdog: bool = True,
    worker_id: str | None = None,
    workers_constructors: dict | None = None,
    watchdog_timeout: float = 60,
    creation_processes_timeout: float = 60,
    allow_remote_constructors: bool = False,
) -> Worker:
    """Builds a node on a filesystem namespace. See `node.node`.

    Equivalent to `node(fsworker(NS_PATH, ...), workers_constructors,
    creation_processes_timeout, allow_remote_constructors)`. The caller
    must call `run()` on the returned master Worker to start serving.

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
        allow_remote_constructors (bool): If True, register
            `create_worker_fn`, which runs code sent by clients. Defaults
            to False.

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
    return node(
        master,
        workers_constructors,
        creation_processes_timeout,
        allow_remote_constructors,
    )


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
    allow_remote_constructors: bool = False,
) -> Worker:
    """Builds a node on a Redis namespace. See `node.node`.

    Equivalent to `node(Worker(RedisConnector(...), worker_id=worker_id),
    workers_constructors, creation_processes_timeout,
    allow_remote_constructors)`. The caller must
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
        allow_remote_constructors (bool): If True, register
            `create_worker_fn`, which runs code sent by clients. Defaults
            to False.

    Returns:
        Worker: The master Worker, already registered. Call its `run`
            method to start serving.

    """
    from .redis_connector import RedisConnector

    connector = RedisConnector(redis_host, redis_port, redis_db, namespace, serializer)
    if clean:
        connector.clean_namespace()
    master = Worker(connector, worker_id=worker_id)
    return node(
        master,
        workers_constructors,
        creation_processes_timeout,
        allow_remote_constructors,
    )
