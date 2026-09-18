import logging

import pytest

from distributed_processing.serializers import (
    JoblibSerializer,
    JsonSerializer,
    PickleSerializer,
)

# Types JSON cannot carry: tuples, bytes, non-string keys.
RICH_OBJ = {"t": (1, 2), "b": b"\x00\xff", 3: {"nested": [1.5, None]}}


def test_json_serializer_round_trip():
    s = JsonSerializer()
    obj = {"method": "add", "args": [1, 2], "id": "c:1"}
    data = s.dumps(obj)
    assert isinstance(data, bytes)
    assert s.loads(data) == obj


def test_pickle_serializer_keeps_python_types():
    s = PickleSerializer()
    data = s.dumps(RICH_OBJ)
    assert isinstance(data, bytes)
    assert s.loads(data) == RICH_OBJ


def test_pickle_serializer_honors_protocol():
    import pickletools

    data = PickleSerializer(protocol=2).dumps([1, 2])
    opcode, arg, _ = next(pickletools.genops(data))
    assert opcode.name == "PROTO" and arg == 2


@pytest.mark.parametrize("compress", [0, 3])
def test_joblib_serializer_round_trip(compress):
    pytest.importorskip("joblib")
    s = JoblibSerializer(compress=compress)
    data = s.dumps(RICH_OBJ)
    assert isinstance(data, bytes)
    assert s.loads(data) == RICH_OBJ


def test_joblib_serializer_handles_numpy_arrays():
    pytest.importorskip("joblib")
    np = pytest.importorskip("numpy")
    s = JoblibSerializer()
    arr = np.arange(6, dtype="float64").reshape(2, 3)
    out = s.loads(s.dumps({"arr": arr}))["arr"]
    assert out.dtype == arr.dtype
    assert (out == arr).all()


def test_library_does_not_configure_logging():
    # Importing the library must not call basicConfig nor force a level:
    # applications decide handlers, level and format.
    lib_logger = logging.getLogger("distributed_processing")
    assert lib_logger.level == logging.NOTSET
    assert any(isinstance(h, logging.NullHandler) for h in lib_logger.handlers)
