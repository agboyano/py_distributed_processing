from __future__ import annotations

import traceback
from time import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from typing_extensions import TypeGuard


def single_request(
    method: str,
    args: list | None = None,
    kwargs: dict | None = None,
    id: str | None = None,
    reply_to: str | None = None,
    ack: bool | None = None,
    is_notification: bool = False,
    **options: Any,
) -> dict:
    """Builds a single request message (JSON-RPC 2.0 style dict).

    Args:
        method (str): Remote function name.
        args (list, optional): Positional arguments for the remote function.
            Defaults to None.
        kwargs (dict, optional): Keyword arguments for the remote function.
            Defaults to None.
        id (str, optional): Request identifier. Required unless
            `is_notification` is True. Defaults to None.
        reply_to (str, optional): Response queue name to be added to the
            message as the `reply_to` key. Defaults to None (not added).
        ack (bool, optional): True if the worker sends a ack message when
            the request is received. False or None otherwise.
            Defaults to None.
        is_notification (bool): True if is a `notification` (a `request`
            with no `id`). Defaults to False.
        **options: Additional arguments added to the message under the
            'options' key.

    Returns:
        dict: Request message, including a `timing` key with the
            `request_sent` time.

    Raises:
        TypeError: If `id` is None and `is_notification` is False, or if
            both `args` and `kwargs` are given.

    """
    sr: dict = {"method": method}

    args = None if args is None or len(args) == 0 else args
    kwargs = None if kwargs is None or len(kwargs) == 0 else kwargs

    sr["is_notification"] = is_notification

    # Notifications carry no id.
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

    sr["timing"] = {"request_sent": time()}

    return sr


def is_single_request(request: Any) -> TypeGuard[dict]:
    "Returns True if `request` looks like a single request message."
    return isinstance(request, dict) and "method" in request


def is_batch_request(request: Any) -> TypeGuard[list]:
    "Returns True if `request` looks like a batch request (list of requests)."
    return (
        isinstance(request, list) and len(request) > 0 and is_single_request(request[0])
    )


def error_response(
    code: int,
    id: str | None = None,
    with_trace: bool = False,
    message: str = "Undefined error",
) -> dict:
    """Builds an error response message.

    Args:
        code (int): Error code. The standard JSON-RPC 2.0 codes (-32700,
            -32600, -32601, -32602, -32603) get their standard message.
        id (str, optional): Id of the request being answered.
            Defaults to None.
        with_trace (bool): If True, include the current traceback in the
            "trace" key of the error object. Defaults to False.
        message (str): Error message used when `code` is not a standard
            one. Defaults to "Undefined error".

    Returns:
        dict: Response message with the "error" key.

    """
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
        -32603: "Internal RPC error.",
    }

    er = {"id": id, "error": {"code": code, "message": error_codes.get(code, message)}}

    if with_trace:
        er["error"]["trace"] = traceback.format_exc()

    return er


def result_response(result: Any = None, id: str | None = None) -> dict:
    "Builds a result response message for the request with id=id."
    # See the note in error_response about the temporary keys
    # is_notification and reply_to.
    rr: dict = {"result": result}

    if id is not None:
        rr["id"] = id

    return rr


def ack(id: str | None, worker: str, queue: str) -> dict:
    "Builds an ack message for the request with id=id."
    return {"ack": {"id": id, "worker": worker, "queue": queue, "time": time()}}


def is_ack(response: Any) -> bool:
    "Returns True if `response` is an ack message."
    return "ack" in response


def is_single_response(response: Any) -> TypeGuard[dict]:
    "Returns True if `response` looks like a single response message."
    return isinstance(response, dict) and ("result" in response or "error" in response)


def is_error_response(response: Any) -> TypeGuard[dict]:
    "Returns True if `response` is a response message with an error."
    return isinstance(response, dict) and ("error" in response)


def is_batch_response(response: Any) -> TypeGuard[list]:
    "Returns True if `response` looks like a batch response (list of responses)."
    return (
        isinstance(response, list)
        and len(response) > 0
        and is_single_response(response[0])
    )
