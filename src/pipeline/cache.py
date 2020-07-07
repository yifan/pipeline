import logging
import os
from abc import ABC, abstractmethod
from types import SimpleNamespace

import mysql.connector as mysql
import redis
import azure.cosmosdb.table

from .exception import PipelineError

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)


def KindsOfCache():
    return [cls.kind for cls in Cache.__subclasses__()]


def CacheOf(typename):
    for cls in Cache.__subclasses__():
        if cls.is_cls_of(typename):
            return cls


def parse_connection_string(connectionString, no_port=False, no_username=False, defaults={}):
    """ Parse connection string in user:password@host:port format

    >>> parse_connection_string("username:password@host:port")
    namespace(host='host', password='password', port='port', username='username')
    >>> parse_connection_string("username@host")
    namespace(host='host', password=None, port=None, username='username')
    >>> parse_connection_string("username:password@host:port", no_port=True)
    namespace(host='host:port', password='password', port=None, username='username')
    >>> parse_connection_string("password@host:port", no_username=True)
    namespace(host='host', password='password', port='port', username=None)
    """
    *userNameAndPasswordOrEmpty, remaining = connectionString.split('@')
    password = None
    if userNameAndPasswordOrEmpty:
        if no_username:
            username, password = None, userNameAndPasswordOrEmpty[0]
        else:
            username, *passwordOrEmpty = userNameAndPasswordOrEmpty[0].split(':')
            if passwordOrEmpty:
                password = passwordOrEmpty[0]
    else:
        username = None
    if not username:
        username = defaults.get('username')

    port = None
    if no_port:
        host = remaining
    else:
        host, *portOrEmpty = remaining.split(':')
        if portOrEmpty:
            port = portOrEmpty[0]
    if not port:
        port = defaults.get('port')

    return SimpleNamespace(
        host=host,
        port=port,
        username=username,
        password=password,
    )


class Cache(ABC):
    kind = 'NONE'

    def __init__(self, config, logger=logger):
        self.config = config
        self.in_fields = self.config.in_fields.split(',') if self.config.in_fields else []
        self.out_fields = self.config.out_fields.split(',') if self.config.out_fields else []
        self.logger = logger

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--in-fields', type=str,
                            default=os.environ.get('INFIELDS', None),
                            help='database fields to read (comma separated)')
        parser.add_argument('--out-fields', type=str,
                            default=os.environ.get('OUTFIELDS', None),
                            help='database fields to write (comma separated)')

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
    """ MemoryReader read data from memory.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> data = {'key1':{'text':'text1', 'title':'title1'}}
    >>> mem = MemoryCache(SimpleNamespace(in_fields='title', out_fields='text,title', mem=data))
    >>> mem.write('key3', {'text': 'text3', 'title': 'title3'})
    >>> mem.read('key3')
    {'title': 'title3'}
    """
    kind = 'MEM'

    def __repr__(self):
        return 'MemoryCache'

    def read(self, key):
        dct = self.config.mem[key]
        if self.in_fields:
            dct = dict([(k, dct[k]) for k in self.in_fields])
        return dct

    def write(self, key, kvs):
        self.config.mem[key] = kvs


class MySqlCache(Cache):
    """ MySQLReader reads data from MySQL

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> MySqlCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text"])
    >>> with patch('mysql.connector.connect') as c:
    ...     MySqlCache(config, "text")
    MySqlCache(localhost:3306/database/table):['text']:[]
    """
    kind = 'MYSQL'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'MySqlCache({}/{}/{}):{}:{}'.format(
            self.mysqlConfig.host,
            self.config.database,
            self.config.table,
            self.in_fields,
            self.out_fields,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--mysql', type=str,
            default=os.environ.get('MYSQL', 'localhost:3306'),
            help='mysql database host:port'
        )
        parser.add_argument(
            '--database', type=str,
            default=os.environ.get('MYSQLDATABASE', 'database'),
            help='muysql database'
        )
        parser.add_argument(
            '--table', type=str,
            default=os.environ.get('MYSQLTABLE', 'table'),
            help='mysql database table'
        )
        parser.add_argument(
            '--keyname', type=str,
            default=os.environ.get('KEYNAME', 'id'),
            help='database field name of key'
        )

    def setup(self):
        self.mysqlConfig = parse_connection_string(self.config.mysql, no_port=True)
        self.db = mysql.connect(
            host=self.mysqlConfig.host,
            user=self.mysqlConfig.username,
            passwd=self.mysqlConfig.password,
            database=self.config.database,
        )
        self.cursor = self.db.cursor()

    def read(self, key):
        query = "SELECT {} FROM {} WHERE {} = '{}'".format(
            self.config.in_fields,
            self.config.table,
            self.config.keyname,
            key
        )
        self.cursor.execute(query)
        r = self.cursor.fetchone()
        if r is None:
            raise PipelineError('Data with key is not found')
        return dict(zip(
            self.config.in_fields.split(','),
            r
        ))

    def write(self, key, kvs):
        query = "UPDATE {} SET {} WHERE {} = '{}'".format(
            self.config.table,
            ",".join(["{} = {}".format(k, v) for k, v in kvs.items()]),
            self.config.keyname,
            key
        )
        self.cursor.execute(query)
        self.db.commit()


class RedisCache(Cache):
    """ RedisCache reads/writes data from/to Redis

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> RedisCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text,title"])
    >>> with patch('redis.Redis') as c:
    ...     RedisCache(config)
    RedisCache(localhost:6379):['text', 'title']:[]
    """
    kind = 'REDIS'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'RedisCache({}:{}):{}:{}'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.in_fields,
            self.out_fields,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--redis', type=str,
            default=os.environ.get('REDIS', 'localhost:6379'),
            help='redis host:port'
        )
        parser.add_argument(
            '--expire', type=int,
            default=os.environ.get('REDISEXPIRE', 7*86400),
            help='expire time for database (default: 7 days)'
        )

    def setup(self):
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )

    def read(self, key):
        """ entries are stored as following in redis:
            a set is managed for each key to contain fields available
            a key:field -> value for accessing field for each key

            TODO: raise error if fields are not available
        """
        results = self.redis.mget(['{}:{}'.format(key, field) for field in self.in_fields])
        return dict(zip(self.fields, results))

    def write(self, key, kvs):
        """ entries are stored as following in redis:
            a set is managed for each key to contain fields available
            a key:field -> value for accessing field for each key

            TODO: check error after mset
        """
        self.redis.mset(
            dict([(key, k) for k, v in kvs.items()]),
        )
        for key in kvs.keys():
            self.redis.expire(key, self.config.expire)


class AzureTableCache(Cache):
    """ AzureTableCache reads/writes data from/to Azure Table

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> AzureTableCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text,title"])
    >>> with patch('azure.cosmosdb.table.TableService') as c:
    ...     AzureTableCache(config)
    AzureTableCache:['text', 'title']:[]
    """
    kind = 'AZURE'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'AzureTableCache:{}:{}'.format(self.in_fields, self.out_fields)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--azuredb', type=str,
            default=os.environ.get('AZUREDB', ''),
            help='azure table connection string'
        )
        parser.add_argument(
            '--table', type=str,
            default=os.environ.get('TABLE', ''),
            help='azure table table name'
        )

    def setup(self):
        self.service = azure.cosmosdb.table.TableService(
            endpoint_suffix="table.cosmos.azure.com",
            connection_string=self.config.azuredb
        )

    def read(self, key):
        """ entries are stored as following in redis:
            a set is managed for each key to contain fields available
            a key:field -> value for accessing field for each key

            TODO: raise error if fields are not available
        """
        entity = self.service.get_entity(
            self.config.table,
            'key',  # PartitionKey
            key,  # RowKey
            timeout=5.0,
        )
        if self.in_fields:
            fields = self.in_fields
        else:
            fields = [k for k in entity.keys() if k not in ('PartitionKey', 'RowKey', 'Timestamp', 'etag')]
        return {k: v for k, v in entity.items() if k in fields}

    def write(self, key, kvs):
        """ entries are stored as following in redis:
            a set is managed for each key to contain fields available
            a key:field -> value for accessing field for each key
        """
        dct = {
            'PartitionKey': 'key',
            'RowKey': key,
        }
        dct.update(kvs)
        self.service.insert_or_merge_entity(
            self.config.table,
            dct,
            timeout=5.0,
        )
