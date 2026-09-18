import json
from typing import Any


class JsonSerializer:
    """Serializes messages to/from UTF-8 encoded JSON bytes.

    Default serializer of `RedisConnector`. Any object with the same
    `dumps`/`loads` pair can replace it (the `pickle`, `dill` or `msgpack`
    modules, for example).
    """

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf8"))
