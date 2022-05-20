
class GenericError(Exception):
    
    def __init__(self, value):
        super(GenericError,self).__init__()
        self.value = value

    def __str__(self):
        return repr(self.value)


class ServerError(GenericError):
    """General Server  Error"""
    pass

class NotFoundError(GenericError):
    """Method not found at service"""
    
    @property
    def resource(self):
        return self.value

class JSONWSPError(GenericError):

    @property
    def response(self):
        return self.value

class DeclarationError(GenericError):
    pass    


class ClientError(GenericError):
    pass
