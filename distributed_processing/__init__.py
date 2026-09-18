import logging

from .async_result import AsyncResult, gather
from .client import Client
from .connector import Connector
from .exceptions import RemoteException
from .serializers import JoblibSerializer, JsonSerializer, PickleSerializer
from .worker import Worker

# Connectors are not imported here: they depend on optional extras
# (distributed-processing[redis] / [fs]). Import them explicitly:
#   from distributed_processing.redis_connector import RedisConnector
#   from distributed_processing.filesystem_connector import FileSystemConnector

__version__ = "0.0.3a0"

# Library best practice: attach a NullHandler and let the application
# configure logging (handlers, level and format).
logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "AsyncResult",
    "Client",
    "Connector",
    "JoblibSerializer",
    "JsonSerializer",
    "PickleSerializer",
    "RemoteException",
    "Worker",
    "gather",
    "__version__",
]
