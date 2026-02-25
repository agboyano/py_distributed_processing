import random
import logging
import redis


logger = logging.getLogger(__name__)


class RedisConnector(object):
    def __init__(self, redis_host="localhost", redis_port=6379, redis_db=0, namespace="tasks"):

        self.connection = redis.Redis(redis_host, redis_port, redis_db)
        self.namespace = namespace

    def clean_namespace(self):
        "Borra todos los objetos vinculados al namespace"
        for item in self.connection.scan_iter(f"{self.namespace}:*"):
            self.connection.delete(item)

    def get_requests_queue(self, queue_name):
        return f"{self.namespace}:requests:{queue_name}"

    def get_client_id(self):
        nclients = str(self.connection.incr(f"{self.namespace}:nclients", 1))
        return f"{self.namespace}:redis_client:{nclients}"

    def get_server_id(self):
        nservers = str(self.connection.incr(f"{self.namespace}:nservers", 1))
        return f"{self.namespace}:redis_server:{nservers}"

    def get_responses_queue(self, client_id):
        return f"{client_id}:responses"

    def get_reply_to_from_id(self, id_str):
        return ":".join(id_str.split(":")[:-1]) + ":responses"

    def methods_registry(self):
        """
        Lo usa el cliente.
        Cada método tiene un set de redis con clave {namespace}:method_queues:{method}.
        El contenido del set son los nombres de las colas donde se puden enviar
        los request para elecutar ese método.
        """

        registry = {}
        for method_set in self.connection.scan_iter(f"{self.namespace}:method_queues:*"):
            method = method_set.decode("utf8").split(":")[-1]
            available = [x.decode("utf8") for x in self.connection.smembers(method_set)]
            registry[method] = available

        return registry

    def register_methods(self, requests_queues_dict):
        """
        Lo usa el servidor para registrar los métodos.
        qrequests_queues_dict es un diccionario  con el nombre (corto) de las colas
        de clave y un diccionario con los nombres de las funciones de claves y la función de valor
        """
        registry = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                if method in registry:
                    registry[method] += [queue_name]
                else:
                    registry[method] = [queue_name]

        for method in registry:
            method_set = f"{self.namespace}:method_queues:{method}"
            self.connection.sadd(method_set, *registry[method])
            colas = ", ".join(str(q) for q in registry[method])
            logger.info(f"Method {method} published as available for queues: {colas}")

    def random_queue_for_method(self, method):
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method):
        method_set = f"{self.namespace}:method_queues:{method}"
        return [x.decode("utf8") for x in self.connection.smembers(method_set)]

    def enqueue(self, queue, msg):
        self.connection.rpush(queue, msg)

    def pop(self, queue, timeout=-1):
        """
        timeout=0 indefinido
        Lo usa el cliente.
        """
        # blpop timeout ==0 waits indefinitely
        return self.connection.blpop(queue, timeout=max(timeout, 0))

    def pop_multiple(self, queues, timeout=-1):
        """
        timeout: Maximum wait time in seconds (< 0 = wait indefinitely, 0 = try once)
        Queues ordenadas por prioridad. 
        Devuelve None si timeout, si no devuelve cola, valor.
        Lo usa el worker.
        """
        # blpop timeout ==0 waits indefinitely
        request_redis = self.connection.blpop(queues, timeout=max(timeout, 0))
        if request_redis is not None:
            return request_redis[0].decode("utf8"), request_redis[1]

        return None

    def pop_all(self, queue):
        """
        Extrae de la cola y devuelve todos los mensajes disponibles en la cola queue. 
        Lo usa el cliente
        """

        pipe = self.connection.pipeline()
        pipe.lrange(queue, 0, -1)
        pipe.delete(queue)
        return pipe.execute()[0]
