import json
from typing import Any


class JsonSerializer:
    """Serializes messages to/from UTF-8 encoded JSON bytes."""

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf8"))


class DummySerializer:
    """Pass-through serializer for connectors that serialize natively."""

    def dumps(self, obj: Any) -> Any:
        return obj

    def loads(self, data: Any) -> Any:
        return data
