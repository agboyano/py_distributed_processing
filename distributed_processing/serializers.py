"""Serializers for connectors that store bytes (`RedisConnector`).

All of them share the same contract: `dumps(obj) -> bytes` and
`loads(bytes) -> obj`. Any object with that pair works too (the `pickle`,
`dill` or `msgpack` modules, for example). Every client and worker on a
namespace must use the same serializer.
"""

from __future__ import annotations

import io
import json
import pickle
from typing import Any


class JsonSerializer:
    """Serializes messages to/from UTF-8 encoded JSON bytes.

    Default serializer of `RedisConnector`. Portable and readable, but
    limited to JSON types: tuples become lists, bytes and NumPy objects
    are rejected.
    """

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf8"))


class PickleSerializer:
    """Serializes messages with the standard `pickle` module.

    Keeps Python types (tuples, bytes, datetimes, custom classes...). Only
    for trusted infrastructure: `pickle.loads` executes code from the data.

    Args:
        protocol (int, optional): Pickle protocol. Defaults to None
            (`pickle.DEFAULT_PROTOCOL`). Lower it if clients and workers
            run different Python versions.

    """

    def __init__(self, protocol: int | None = None):
        self.protocol = protocol

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=self.protocol)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)


class JoblibSerializer:
    """Serializes messages with `joblib` (efficient for NumPy and pandas).

    Requires `joblib` (installed with the `fs` extra, as a dependency of
    `fs_structs`). Same trust caveat as `PickleSerializer`: joblib is
    pickle underneath.

    Args:
        compress: `compress` argument of `joblib.dump` (0 to 9, or a
            (method, level) tuple). Defaults to 0 (no compression).

    """

    def __init__(self, compress: int | bool | tuple = 0):
        import joblib  # optional dependency

        self._joblib = joblib
        self.compress = compress

    def dumps(self, obj: Any) -> bytes:
        buffer = io.BytesIO()
        self._joblib.dump(obj, buffer, compress=self.compress)
        return buffer.getvalue()

    def loads(self, data: bytes) -> Any:
        return self._joblib.load(io.BytesIO(data))
