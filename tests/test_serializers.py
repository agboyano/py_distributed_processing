import logging

from distributed_processing.serializers import DummySerializer, JsonSerializer


def test_json_serializer_round_trip():
    s = JsonSerializer()
    obj = {"method": "add", "args": [1, 2], "id": "c:1"}
    data = s.dumps(obj)
    assert isinstance(data, bytes)
    assert s.loads(data) == obj


def test_dummy_serializer_is_identity():
    s = DummySerializer()
    obj = {"anything": [1, 2, 3]}
    assert s.dumps(obj) is obj
    assert s.loads(obj) is obj


def test_library_does_not_configure_logging():
    # Importing the library must not call basicConfig nor force a level:
    # applications decide handlers, level and format.
    lib_logger = logging.getLogger("distributed_processing")
    assert lib_logger.level == logging.NOTSET
    assert any(isinstance(h, logging.NullHandler) for h in lib_logger.handlers)
