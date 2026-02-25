import traceback
from time import time


def single_request(method, args=None, kwargs=None, id=None, reply_to=None, ack=None, is_notification=False, **options):
    sr = {"method": method}

    args = None if args is None or len(args) == 0 else args
    kwargs = None if kwargs is None or len(kwargs) == 0 else kwargs

    sr["is_notification"] = is_notification

    # Si es una notificación no incluimos id si es None

    if id is None and not is_notification:
        raise TypeError("id must be not null if not a notification")

    if id is not None:
        sr["id"] = id

    if reply_to is not None:
        sr["reply_to"] = reply_to

    if ack is not None:
        sr["ack"] = ack

    if args is not None and args != []:
        sr["args"] = args[:]

    if kwargs is not None and kwargs != {}:
        sr["kwargs"] = kwargs

    if "args" in sr and "kwargs" in sr:
        raise TypeError("Only allowed either positional or named parameters")

    if len(options) > 0:
        sr["options"] = options
    
    sr["timing"] = {"request_sent":time()}

    return sr


def is_single_request(request):
    return isinstance(request, dict) and "method" in request


def is_batch_request(request):
    return isinstance(request, list) and len(request) > 0 and is_single_request(request[0])


def error_response(code, id=None, with_trace=False, message="Undefined error"):
    # is_notification is temporarily necessary in a response 
    # because it is used to decide, once the request is processed, 
    # whether to send the response or not. Should be deleted before
    # sending the message. 
    # Similarly, reply_to is not needed in the response and it could 
    # be misinterpreted, as is related with the reply_to queue of the original 
    # request. It is only used to keep track in the worker's code of the queue where 
    # the response has to be sent.
    # 
    # NOT PRETTY

    error_codes = {
        -32700: "Parse error.",
        -32600: "Invalid Request.",
        -32601: "The method does not exist/is not available.",
        -32602: "Invalid method parameters.",
        -32603: "Internal RPC error."}

    er = {
        "id": id,
        "error": {"code": code, "message": error_codes.get(code, message)}}

    if with_trace:
        er["error"]["trace"] = traceback.format_exc()

    return er


def result_response(result=None, id=None):
    # is_notification is temporarily necessary in a response 
    # because it is used to decide, once the request is processed, 
    # whether to send the response or not. Should be deleted before
    # sending the message. 
    # Similarly, reply_to is not needed in the response and it could 
    # be misinterpreted, as is related with the reply_to queue of the original 
    # request. It is only used to keep track in the worker's code of the queue where 
    # the response has to be sent.
    # 
    # NOT PRETTY
    rr = {"result": result}

    if id is not None:
        rr["id"] = id

    return rr

def ack(id, worker, queue):
    return {"ack":{"id":id, "worker":worker, "queue":queue, "time":time()}}

def is_ack(response):
    return "ack" in response

def is_single_response(response):
    return isinstance(response, dict) and ("result" in response or "error" in response)

def is_error_response(response):
    return  isinstance(response, dict) and ("error" in response)

def is_batch_response(response):
    return isinstance(response, list) and len(response) > 0 and is_single_response(response[0])
