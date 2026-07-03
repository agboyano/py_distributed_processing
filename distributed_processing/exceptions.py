class RemoteException(Exception):
    """Raised by the client when a response contains an "error" object."""

    def __init__(self, error: dict):
        self.code = error.get("code", "00000")
        self.message = error.get("message", "Unidentified error")
        self.trace = error.get("trace", "")

    def __str__(self) -> str:
        return f"Error {self.code} : {self.message}\n\n {self.trace}"
