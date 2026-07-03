# distributed-processing

Librería ligera de computación distribuida sobre colas de mensajes, con un
protocolo RPC inspirado en [JSON-RPC 2.0](https://www.jsonrpc.org/specification).

Un **worker** registra funciones y escucha en una o varias colas de peticiones;
un **cliente** descubre qué métodos hay disponibles (registro), envía peticiones
y recoge las respuestas de forma síncrona o asíncrona (`AsyncResult`). El
transporte es intercambiable mediante **conectores**: Redis o sistema de
ficheros compartido (NFS, disco local, etc.).

## Características

- RPC síncrono y asíncrono (`rpc_sync`, `rpc_async`, `AsyncResult`).
- Peticiones batch (un solo mensaje, un solo worker) y multi (repartidas entre
  workers): `rpc_batch_*`, `rpc_multi_*`.
- Registro de métodos: los clientes descubren qué colas sirven cada método
  (`check_registry="cache" | "always" | otro`).
- Colas con prioridades; las de igual prioridad se barajan en cada iteración.
- Notificaciones (peticiones sin respuesta), acks opcionales y reintentos
  (`AsyncResult.retry`, `gather`).
- Envío de funciones Python arbitrarias serializadas con `dill`
  (`rpc_async_fn` + `Worker.add_python_eval`). **Ver nota de seguridad.**
- Conectores: Redis (`RedisConnector`) y sistema de ficheros
  (`FileSystemConnector`, basado en `fs_structs`, con espera por eventos
  de watchdog o polling).

## Instalación

```bash
pip install .              # núcleo (dill)
pip install .[redis]       # + conector Redis
pip install .[fs]          # + conector de sistema de ficheros (fs_structs, watchdog)
pip install .[dev]         # + pytest
```

`fs_structs` es un proyecto hermano no publicado en PyPI; instálalo en local,
por ejemplo: `pip install -e ../fs_structs`.

## Uso rápido

Worker (un proceso):

```python
from distributed_processing import Worker, JsonSerializer
from distributed_processing.redis_connector import RedisConnector

def add(a, b):
    return a + b

worker = Worker(JsonSerializer(), RedisConnector("localhost"))
worker.add_requests_queue("mi_cola", {"add": add})
worker.update_methods_registry()
worker.run()          # escucha indefinidamente; run(timeout=60) para acotar
```

Cliente (otro proceso):

```python
from distributed_processing import Client, JsonSerializer
from distributed_processing.redis_connector import RedisConnector

client = Client(JsonSerializer(), RedisConnector("localhost"))

client.rpc_sync("add", [1, 2])          # → 3, bloqueante

f = client.rpc_async("add", [20, 22])   # AsyncResult
f.get(timeout=10)                       # → 42

fs = client.rpc_multi_async([("add", [i, i], None) for i in range(100)])
[f.safe_get(timeout=60) for f in fs]    # repartido entre los workers
```

Con el conector de sistema de ficheros basta con compartir un directorio:

```python
from distributed_processing.utils import fsworker, fsclient

worker = fsworker("/ruta/compartida/ns")   # + add_requests_queue + run()
client = fsclient("/ruta/compartida/ns")
```

Para lanzar un nodo con varios workers en subprocesos gestionados remotamente
(crear/listar/matar workers vía RPC), ver `distributed_processing.utils.fsnode`.

## Nota de seguridad

`Worker.add_python_eval()` expone `eval_py_function`, que deserializa con
`dill` y **ejecuta código Python arbitrario** enviado por los clientes
(es lo que usa `rpc_async_fn`). Cualquiera con acceso de escritura a las colas
(el servidor Redis o el directorio compartido) puede ejecutar código en los
workers. Úsalo únicamente en infraestructura de confianza y no expongas el
transporte a redes no confiables.

## Protocolo

Mensajes estilo JSON-RPC 2.0 con extensiones: `reply_to` (cola de respuesta),
`ack` (confirmación de recepción), `is_notification`, `options` y `timing`/
`metadata` (worker, cola, tiempos de ejecución). Códigos de error estándar:
`-32600` petición inválida, `-32601` método no encontrado, `-32602` parámetros
inválidos, `-32603` error interno (incluye la traza remota si el worker se
crea con `with_trace=True`).

## Tests y calidad

```bash
pytest                       # suite completa (~2 s)
pytest -m "not integration"  # solo unitarios (sin tocar el filesystem)
ruff check distributed_processing tests   # lint
ruff format distributed_processing tests  # formateo
```

Los tests unitarios usan un conector en memoria (`tests/conftest.py`), sin
necesidad de Redis ni de un directorio compartido. La CI (GitHub Actions)
ejecuta lint + tests en Python 3.9–3.13.

## Estructura

```
distributed_processing/
├── client.py                # Client: envío de peticiones, caché de respuestas
├── worker.py                # Worker: colas, despacho y ejecución de métodos
├── async_result.py          # AsyncResult y gather()
├── messages.py              # construcción y validación de mensajes
├── serializers.py           # JsonSerializer, DummySerializer
├── redis_connector.py       # transporte Redis
├── filesystem_connector.py  # transporte por sistema de ficheros (fs_structs)
├── exceptions.py            # RemoteException
└── utils.py                 # fsworker/fsclient/fsnode (helpers filesystem)
```

En `examples/` hay notebooks de uso (filesystem, Redis y un caso real de
Monte Carlo para autocallables).
