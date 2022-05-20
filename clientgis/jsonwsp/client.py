import json
try:
    from httplib import HTTPConnection
except:
    from http.client import HTTPConnection

from jsonwsp.exceptions import ClientError


class ServiceMethod(object):

    def __init__(self, client, name):
        self.client = client
        self.name = name

    def __call__(self, *args, **kw):
        request = self.client.build_request(self.name, args, kw)
        response = self.client.send_request(request)
        return response['result']


class ServiceClient(object):

    def __init__(self, client):
        self._client = client
        methods = {}
        method_names = client.get_method_names()

        for key in method_names:
            methods[key] = ServiceMethod(client, key)

        self._methods = methods

    def __getattribute__(self, name):
        methods = object.__getattribute__(self, '_methods')
        if methods.has_key(name):
            return self._methods[name]
        else:
            return object.__getattribute__(self, name)


class ServiceConnection(object):
    version = '1.0'

    def __init__(self, host, port, path):
        self.host = host
        self.path = path
        self.port = port
        self.connection = None
        self.description = None

    def initialize(self):
        """
        Setup HTTP connection and fetch service description.
        """
        self.connection = HTTPConnection(self.host, self.port)
        self.fetch_description()

    def fetch_description(self):
        self.connection.request('GET', "%s/%s" % (self.path, 'description.json'))
        response = self.connection.getresponse()

        if response.status == 200:
            description = response.read()
            self.parse_description(description)

    def parse_description(self, json_body):
        req_dict = json.loads(json_body.encode('utf-8'))
        self.description = req_dict

    def build_request(self, name, args, kw):
        request_args = {}

        for i, arg in enumerate(args):
            key = self._get_param_by_index(name, i+1)
            request_args[key] = arg

        for key, value in kw.items():
            request_args[key] = value

        request = {}
        request['type'] = 'jsonwsp/response'
        request['version'] = self.version
        request['methodname'] = name
        request['args'] = request_args
        return json.dumps(request, ensure_ascii=False).encode('utf-8')

    def send_request(self, request):
        clen = len(request)
        header = {'Content-Type': 'application/json', 'Content-Length': clen}
        self.connection.request('POST', self.path, request, header)
        response = self.connection.getresponse()
        data = response.read()
        return json.loads(data.encode('utf-8'))

    def get_method_names(self):
        if 'methods 'in self.description:
            return self.description['methods'].keys()
        else:
            raise ClientError("Connection not initialized")

    def get_method(self,name):

        if 'methods' in self.description:

            if self.description['methods'].has_key(name):
                return ServiceMethod(self,name)
            else:
                raise ClientError("Service at \"%s\" doesn't expose method with name \"%s\""
                     % (self.path, name))
        else:
            raise ClientError("Connection not initialized")

    def get_service(self):
        return ServiceClient(self)

    def _get_param_by_index(self, methodname, index):
        params = self.description['methods'][methodname]['params']
        for key in params:
            if int(params[key]['def_order']) == index:
                return key

        return None

    def _get_param_by_name(self, methodname, name):
        params = self.description['methods'][methodname]['params']
        return params.get(name, None)
