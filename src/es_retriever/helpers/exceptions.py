class EsretrieverError(Exception):
    def __init__(self, message):
        self.message = message
        Exception.__init__(self, message)


class EsretrieverUnautorizedAccess(EsretrieverError):
    def __init__(self):
        self.message = "Unautorized access to Elastic Search"
        EsretrieverError.__init__(self, self.message)

