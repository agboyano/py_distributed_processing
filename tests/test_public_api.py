import distributed_processing


def test_public_api_exports():
    from distributed_processing import (  # noqa: F401
        AsyncResult,
        Client,
        DummySerializer,
        JsonSerializer,
        RemoteException,
        Worker,
        gather,
    )


def test_version():
    assert isinstance(distributed_processing.__version__, str)
    assert distributed_processing.__version__ != ""


def test_all_matches_module_attributes():
    for name in distributed_processing.__all__:
        assert hasattr(distributed_processing, name)
