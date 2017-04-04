from collections import namedtuple
from functools import partial
from unittest.mock import patch

from aiohttp.streams import StreamReader


ClientRequest = namedtuple('ClientRequest', ['method', 'url', 'headers'])


class Request:
    def __init__(self, method, url, *args, **kwargs):
        self.method = method
        self.url = url
        self.args = args
        self.kwargs = kwargs
        self.content = StreamReader()
        self.content.feed_data(kwargs.get('content'))
        self.content.feed_eof()

    def match(self, method, url):
        if method == self.method:
            if url == self.url:
                return True

    @property
    def status(self):
        return self.kwargs.get('status') or 200

    @property
    def headers(self):
        return self.kwargs.get('headers', {})

    def raise_for_status(self): ...

    async def release(self): ...

    async def json(self):
        return self.kwargs.get('json')

    async def text(self):
        return self.kwargs.get('text')


class Mock:
    def __init__(self):
        self.requests = []
        self.request_history = []

    def __enter__(self):
        self.patch = patch('aiohttp.ClientSession')
        Session = self.patch.start()
        instance = Session.return_value
        instance.request.side_effect = self._request
        instance.get.side_effect = partial(self._request, 'GET')
        instance.put.side_effect = partial(self._request, 'PUT')
        instance.post.side_effect = partial(self._request, 'POST')
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.patch.stop()

    @property
    def call_count(self):
        return len(self.request_history)

    @property
    def called(self):
        return self.call_count > 0

    async def _request(self, method, url, *args, **kwargs):
        self.request_history.append(
            ClientRequest(method=method,
                          url=url,
                          headers=kwargs.get('headers')))
        for r in self.requests:
            if r.match(method, url):
                return r
        raise Exception('No request matching {}: {}'.format(method, url))

    def request(self, method, url, *args, **kwargs):
        self.requests.append(Request(method, url, *args, **kwargs))

    def get(self, url, *args, **kwargs):
        self.request('GET', url, *args, **kwargs)

    def put(self, url, *args, **kwargs):
        self.request('PUT', url, *args, **kwargs)

    def post(self, url, *args, **kwargs):
        self.request('POST', url, *args, **kwargs)
