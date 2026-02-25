import json
import logging

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


class JsonSerializer:
    def dumps(self, obj):
        return json.dumps(obj).encode("utf8")

    def loads(self, json_str):
        return json.loads(json_str.decode("utf8"))
    
class DummySerializer:
    def dumps(self, obj):
        return obj

    def loads(self, str):
        return str
