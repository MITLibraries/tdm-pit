import aioes


__version__='0.3.0'


# Monkeypatch for aioes.
# This is a temporary fix for https://github.com/aio-libs/aioes/issues/112.

_old_connection_init = aioes.connection.Connection.__init__

def __new__init__(self, *args, **kwargs):
    _old_connection_init(self, *args, **kwargs)
    self._base_url = self._base_url.rstrip('/')

aioes.connection.Connection.__init__ = __new__init__
