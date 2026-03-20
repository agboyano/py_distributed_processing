import argparse
import datetime
from math import exp, sqrt
import numpy as np
from distributed_processing.serializers import JsonSerializer
from distributed_processing.worker import Worker
from distributed_processing.redis_connector import RedisConnector
import mc_autocall_mapper


REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
NAMESPACE = "montecarlos"




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, default=REDIS_HOST, help="Redis server address")
    parser.add_argument('-p', '--port', type=int, default=REDIS_PORT, help="Redis server port")
    parser.add_argument('-db', type=int, default=REDIS_DB, help="Redis server DB")
    parser.add_argument('-n', '--namespace', type=str, default=NAMESPACE, help="Namespace to use")
    parser.add_argument('--clean', action='store_true', help="Clean namespace")

    args = parser.parse_args()

    print(
        f"Connecting to Redis server in {args.host}: {args.port}, DB {args.db} and Namespace {args.namespace}")

    redis_connector = RedisConnector(redis_host=args.host, redis_port=args.port,
                                     redis_db=args.db, namespace=args.namespace)

    if args.clean:
        print(f"Cleaning namespace {args.namespace}")
        redis_connector.clean_namespace()

    server = Worker(JsonSerializer(), redis_connector)

    server.add_requests_queue("cola_1", {"mc_autocall_mp":mc_autocall_mapper.mc_autocall_mapper,
                                         "mc_autocall_mp2":mc_autocall_mapper.mc_autocall_mapper2,
                                         "mc_autocall_empty":mc_autocall_mapper.mc_autocall_empty_mapper})

    server.update_methods_registry()

    server.run()
