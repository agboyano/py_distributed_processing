from .serializers import DummySerializer
from .client import Client
from .worker import Worker
from .filesystem_connector import FileSystemConnector


def fsclient(NS_PATH, check_registry="Cache", with_watchdog= True, pop_watchdog_timeout=10):
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog=True
    fs_connector.pop_watchdog_timeout = 10
    return Client(DummySerializer(), fs_connector, check_registry="cache")

def fsworker(NS_PATH, clean=False, with_watchdog=True):
    fs_connector = FileSystemConnector(NS_PATH)
    fs_connector.with_watchdog = True
    if clean:
        fs_connector.clean_namespace()

    return Worker(DummySerializer(), fs_connector)
