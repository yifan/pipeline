import os
from abc import ABC, abstractmethod
from types import SimpleNamespace

from .importor import import_class


def supportedCacheKinds():
    return {
        "MEM": MemoryCache,
        "MYSQL": "pipeline.backends.mysql:MySqlCache",
        "REDIS": "pipeline.backends.redis:RedisCache",
        "AZURE": "pipeline.backends.azure:AzureTableCache",
    }


def KindsOfCache():
    return supportedCacheKinds().keys()


def CacheOf(typename):
    try:
        cacheClass = supportedCacheKinds()[typename]
    except IndexError:
        raise TypeError(f"Source type '{typename}' is invalid") from None

    if isinstance(cacheClass, str):
        return import_class(cacheClass)
    else:
        return cacheClass


def parse_connection_string(
    connectionString, no_port=False, no_username=False, defaults={}
):
    """Parse connection string in user:password@host:port format

    >>> parse_connection_string("username:password@host:port")
    namespace(host='host', password='password', port='port', username='username')
    >>> parse_connection_string("username@host")
    namespace(host='host', password=None, port=None, username='username')
    >>> parse_connection_string("username:password@host:port", no_port=True)
    namespace(host='host:port', password='password', port=None, username='username')
    >>> parse_connection_string("password@host:port", no_username=True)
    namespace(host='host', password='password', port='port', username=None)
    """
    *userNameAndPasswordOrEmpty, remaining = connectionString.split("@")
    password = None
    if userNameAndPasswordOrEmpty:
        if no_username:
            username, password = None, userNameAndPasswordOrEmpty[0]
        else:
            username, *passwordOrEmpty = userNameAndPasswordOrEmpty[0].split(":")
            if passwordOrEmpty:
                password = passwordOrEmpty[0]
    else:
        username = None
    if not username:
        username = defaults.get("username")

    port = None
    if no_port:
        host = remaining
    else:
        host, *portOrEmpty = remaining.split(":")
        if portOrEmpty:
            port = portOrEmpty[0]
    if not port:
        port = defaults.get("port")

    return SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
    )


class Cache(ABC):
    kind = "NONE"

    def __init__(self, config, logger):
        self.config = config
        self.in_fields = (
            self.config.in_fields.split(",") if self.config.in_fields else []
        )
        self.out_fields = (
            self.config.out_fields.split(",") if self.config.out_fields else []
        )
        self.logger = logger

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "--in-fields",
            type=str,
            default=os.environ.get("INFIELDS", None),
            help="database fields to read (comma separated)",
        )
        parser.add_argument(
            "--out-fields",
            type=str,
            default=os.environ.get("OUTFIELDS", None),
            help="database fields to write (comma separated)",
        )

    def setup(self):
        """ establish connection for example """

    @abstractmethod
    def read(self, key):
        """ return corresponding *fields* from database with key """

    @abstractmethod
    def write(self, key, kv):
        """ return corresponding *fields* from database with key """

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    def close(self):
        pass


class MemoryCache(Cache):
    """MemoryReader read data from memory.
    It is for testing only.

    >>> import logging
    >>> from types import SimpleNamespace
    >>> data = {'key1':{'text':'text1', 'title':'title1'}}
    >>> mem = MemoryCache(SimpleNamespace(in_fields='title', out_fields='text,title', mem=data), logging)
    >>> mem.write('key3', {'text': 'text3', 'title': 'title3'})
    >>> data
    {'key1': {'text': 'text1', 'title': 'title1'}, 'key3': {'text': 'text3', 'title': 'title3'}}
    >>> mem.read('key3')
    {'title': 'title3'}
    """

    kind = "MEM"

    def __repr__(self):
        return "MemoryCache"

    def read(self, key):
        dct = self.config.mem[key]
        if self.in_fields:
            dct = dict([(k, dct[k]) for k in self.in_fields])
        return dct

    def write(self, key, kvs):
        if key not in self.config.mem:
            self.config.mem[key] = {}
        self.config.mem[key].update(kvs)


class CachedMessageMixin(object):
    """"""

    def get(self, key, default=None):
        value = self.dct.get(key)
        if value is None:
            if not hasattr(self, "fetched"):
                self.fetched = self.cache.read(self.dct[self.keyname])
            value = self.fetched.get(key, default)
        return value

    def update(self, other):
        self.cache.write(self.dct[self.keyname], other)

    def replace(self, other):
        """ Not supported in cache mode """
        raise NotImplementedError(".replace() is not supported in cache mode")


def CachedMessageClass(messageClass, cache):
    return type("CachedMessage", (CachedMessageMixin, messageClass), dict(cache=cache))
