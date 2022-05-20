import json
try:
    from urlparse import urlparse
except:
    from urllib.parse import urlparse

import sys
import inspect

from wsgiref.util import request_uri

from jsonwsp.exceptions import ServerError, NotFoundError, JSONWSPError


class HTTPHandler(object):
    """
    Handles HTTP requests and serves responses. Here are handled GET requests
    for artificial file tree of every service. POST requests are
    forwared to Service and handled there.
    """

    routes = {}

    def __init__(self, start_response):
        self.start_response = start_response

    def get_service(self, path):
        """
        Get service assigned to provided path
        """

        if path in self.routes:
            return self.routes[path]
        else:
            raise NotFoundError(path)

    def handle_request(self, environ, request_body):
        """
        Entry method for every HTTP request. It calls correspondig method
        according to method or raises exception if request is mallformed.
        """

        try:
            #separate path and resource
            uri = request_uri(environ)
            uri = urlparse(uri)
            path_parts = uri.path.split('/')
            method = environ['REQUEST_METHOD']

            if method == 'POST':
                return self._handle_post_request(path_parts, request_body)

            elif method == 'GET':
                return self._handle_get_request(path_parts)

            else:
                raise ServerError()

        except JSONWSPError as exception:
            return self._handle_jsonwsp_error(exception)

        except NotFoundError as exception:
            return self._handle_not_found_error(exception)

        except ServerError as exception:
            return self._handle_server_error(exception.string)

        except:
            return self._handle_server_error(
                "Unexpected error: %s" % sys.exc_info()[0])

    def _render_html_description(self, descr):
        """
        Render description dictionary as HTML
        """
        html = []
        html.append("<h1>%s</h1>" % descr['servicename'])

        html.append("<h2>List of Methods:</h2>")
        html.append("<ul>")

        for key in descr['methods']:
            params_list = []

            for pkey in descr['methods'][key]['params']:
                param = descr['methods'][key]['params'][pkey]
                params_list.append("<b>%s</b> %s" % (param['type'], pkey))

            params_str = ', '.join(params_list)
            doc_lines = descr['methods'][key]['doc_lines']
            html.append("<li>%s(%s) returns <b>%s</b>, %s</li>" % (
                key, params_str, descr['methods'][key]['rtype'], doc_lines))

        html.append("</ul>")

        html.append("<p>JSON Web Service ver. %s</p>" % descr['version'])

        return '\n'.join(html)

    def _handle_server_error(self, message):
        return self._serve_response('text/plain', message,
                                    '500 Internal Server Error')

    def _handle_not_found_error(self, exception):
        return self._serve_response('text/plain', '404 Not Found',
                                    '404 Not Found')

    def _handle_jsonwsp_error(self, exception):
        return self._serve_response('text/json', exception.response,
                                    '404 Not Found')

    def _handle_get_request(self, path_parts):
        """
        handles HTTP GET requests. Via GET request is possible to fetch
        description of a service
        """

        if path_parts[-1] in ['index.html', 'description.html', '']:
            path = '/'.join(path_parts[0:-1])

            service = self.get_service(path)
            description = service.build_description()
            body = self._render_html_description(description)

            return self._serve_response('text/html', body, '200 OK')

        elif path_parts[-1] == 'description.json':
            path = '/'.join(path_parts[0:-1])

            service = self.get_service(path)
            description = service.build_description()
            body = json.dumps(description, ensure_ascii=False).encode('utf-8')

            return self._serve_response('application/json', body, '200 OK')

        else:
            path = '/'.join(path_parts)
            service = self.get_service(path)
            description = service.build_description()
            body = self._render_html_description(description)

            return self._serve_response('text/html', body, '200 OK')

    def _handle_post_request(self, path_parts, request_body):
        """
        Handles POST requests which should contain JSONWSP call request.
        """
        path = '/'.join(path_parts)
        service = self.get_service(path)

        response_body = service.handle_request(request_body)

        return self._serve_response('application/json',
                                    response_body,
                                    '200 OK')

    def _serve_response(self, content_type, body, status):
        """
        Helper method, serves proper HTTP response header.
        """
        header = [('Content-Type', content_type),
                  ('Content-Length', str(len(body)))]
        self.start_response(status, header)
        return [body]


class Service(object):
    """
    Handles JSONWSP requests and holds data necessary to build service
    description. Provides decorators to map service functions, parses
    end executes call requests, builds response dictionaries...
    Everything important is done here.
    """
    version = '1.0'

    def __init__(self, name, uri):
        self.name = name
        self.uri = uri
        self.methods = {}

        HTTPHandler.routes[uri] = self

    def _add_method(self, **kw):
        """
        Helper method for decorators it adds method to self.methods
        dictionary which holds all required metadata for exposing a function.

        This method extracts return type of function its name and pydoc
        documentation.
        """
        name = kw.get('name')
        rtype = kw.get('rtype')
        function = kw.get('function')

        if name is not None:

            if not name in self.methods:
                self.methods[name] = {}

            method_root = self.methods[name]

            if 'rtype' not in method_root or method_root['rtype'] is None:
                method_root['rtype'] = rtype

            if 'function' not in method_root or method_root['function'] is None:
                method_root['function'] = function
                method_root['doc_lines'] = function.__doc__

    def _add_method_params(self, **kw):
        """
        Helper method for decorators it adds method to self.methods
        dictionary which holds all required metadata for exposing a function.

        Method extracts metadata about exposed function arguments.
        """

        name = kw.get('name')
        params = kw.get('params', [])
        named_params = kw.get('named_params', {})
        function = kw.get('function')

        if name is not None:

            if name not in self.methods:
                self.methods[name] = {}

            method_root = self.methods[name]

            params_root = method_root['params'] = {}

            # add non named params
            func_param_names, _, _, _ = inspect.getargspec(function)

            if len(params) != len(func_param_names):
                raise Exception

            for i, key in enumerate(params):
                params_root[func_param_names[i]] = dict(type=params[i],
                                                        doc_lines='',
                                                        def_order=i+1)

            for key in named_params:
                params_root[key] = dict(type=named_params[key],
                                        doc_lines='',
                                        def_order=len(params_root)+1)

    def _convert_args(self, method_name, args):
        """
        Naive implementation of type conversion for function argumetns.
        This is invked before every exposed function call for every
        argument/paramenter.
        """

        params = self.methods[method_name]['params']
        conv_args = {}

        for key in args:
            if key in params:

                if params[key]['type'] is str:
                    conv_args[key] = params[key]['type'](args[key].encode('utf-8'))
                else:
                    conv_args[key] = params[key]['type'](args[key])

        return conv_args

    def params(self, *args, **kw):
        """
        Parameter list decorator. When you are expoosing a function you can
        decribe parameters which are required for function call with correct
        names and types.
        """

        def params_decorator(func):
            self._add_method_params(name=func.__name__, params=args,
                                    named_params=kw, function=func)

            return func

        return params_decorator

    def expose(self, func):
        self._add_method(name=func.__name__, rtype=None, function=func)
        return func

    def rtype(self, rt):
        """
        Return type decorator. With this decorator you assign return type to
        a exposed function
        """

        def rtype_decorator(func):
            self._add_method(name=func.__name__, rtype=rt, function=func)
            return func

        return rtype_decorator

    def build_description(self):
        """
        Builds dictionary with description of the service. This dictionary
        is ready for JSON ecoding.
        """
        descr = {}
        descr['type'] = 'jsonwsp/response'
        descr['version'] = self.version
        descr['servicename'] = self.name
        descr['url'] = ''
        descr['methods'] = {}

        for mkey in self.methods:
            descr['methods'][mkey] = {}
            descr['methods'][mkey]['rtype'] = self.methods[mkey]['rtype'].__name__
            descr['methods'][mkey]['doc_lines'] = self.methods[mkey]['doc_lines']
            params = descr['methods'][mkey]['params'] = {}

            for i,pkey in enumerate(self.methods[mkey]['params']):
                params[pkey] = {}
                params[pkey]['type'] = self.methods[mkey]['params'][pkey]['type'].__name__
                params[pkey]['doc_lines'] = str(self.methods[mkey]['params'][pkey][
                    'doc_lines'])
                params[pkey]['def_order'] = self.methods[mkey]['params'][pkey]['def_order']

        return descr

    def build_json_description(self):
        """
        Returns description of the service encoded in JSON.
        """
        descr = self.build_description()
        return json.dumps(descr, ensure_ascii=False).encode('utf-8')

    def parse_request(self, json_body, encoding):
        """
        Convers JSON request into python dictionary
        """
        req_dict = json.loads(json_body.decode('utf-8'))
        return req_dict

    def build_response(self, method_name, result):
        """
        Bncapsulace result of method call into JSON response.
        """
        res_dict = {}
        res_dict['type'] = 'jsonwsp/response'
        res_dict['version'] = self.version
        res_dict['servicename'] = self.name
        res_dict['method'] = method_name

        if isinstance (result, str):
            res_dict['result'] = result.decode('utf-8')
        else:
            res_dict['result'] = result
            

        return json.dumps(res_dict, ensure_ascii=False).encode('utf-8')

    def build_error_response(self, code, string, detail=None, filename=None,
                             lineno=None):
        """
        Builds error response in JSON format
        """
        res_dict = {}
        res_dict['type'] = 'jsonwsp/error'
        res_dict['version'] = self.version
        res_dict['code'] = code
        res_dict['string'] = string

        return json.dumps(res_dict, ensure_ascii=False).encode('utf-8')

    def handle_request(self, request_body):
        """
        Process JSON request. Returns JSONWSP response or throws an exception.
        """
        try:
            request = self.parse_request(request_body, 'utf-8')
            method_name = request['methodname']
            req_args = request['args']

        except:
            error_response = self.build_error_response('client',
                                                       "Request malformed")
            raise JSONWSPError(error_response)

        if method_name in self.methods:

            try:
                args = self._convert_args(method_name, req_args)
                result = self.methods[method_name]['function'](**args)

                return self.build_response(method_name, result)

            except:
                error_response = self.build_error_response('server',
                                                           "Method call faild")
                raise #JSONWSPError(error_response)

        else:
            error_response = self.build_error_response('client',
                                                       "No such method")
            raise JSONWSPError(error_response)


def application(environ, start_response):

    handler = HTTPHandler(start_response)

    try:
        request_body_size = int(environ.get('CONTENT_LENGTH', 0))

    except (ValueError):
        request_body_size = 0

    request_body = environ['wsgi.input'].read(request_body_size)

    return handler.handle_request(environ, request_body)
