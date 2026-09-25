import distributed_processing


def test_public_api_exports():
    from distributed_processing import (  # noqa: F401
        AsyncResult,
        Client,
        Connector,
        JoblibSerializer,
        JsonSerializer,
        PickleSerializer,
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


def test_utils_reexports_the_node_engine():
    # utils imports no connector at module level, so this needs no extra.
    from distributed_processing import node as node_module
    from distributed_processing.utils import (  # noqa: F401
        deserialize,
        fsnode,
        node,
        redisnode,
        serialize,
    )

    assert node is node_module.node
    assert serialize is node_module.serialize
