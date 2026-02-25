class RemoteException(Exception):
    def __init__(self, error):

        self.code = "00000"
        self.message = "Error no identificado"
        self.trace = ""

        if "code" in error:
            self.code = error["code"]
        if "message" in error:
            self.message = error["message"]
        if "trace" in error:
            self.trace = error["trace"]

    def __str__(self):
        return f'Error {self.code} : {self.message}\n\n {self.trace}'
