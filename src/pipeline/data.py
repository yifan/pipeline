import logging
import os
from abc import ABC, abstractmethod

import mysql.connector as mysql
import redis

from .exception import PipelineError
from .utils import parse_connection_string

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)


def KindsOfDataReader():
    return [cls.kind for cls in DataReader.__subclasses__()]


def DataReaderOf(typename):
    for cls in DataReader.__subclasses__():
        if cls.is_cls_of(typename):
            return cls


def DataWriterOf(typename):
    for cls in DataWriter.__subclasses__():
        if cls.is_cls_of(typename):
            return cls


class DataReader(ABC):
    kind = 'NONE'

    def __init__(self, config, logger=logger):
        self.config = config
        self.fields = self.config.in_fields.split(',')
        self.logger = logger

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--in-fields', type=str,
                            default=os.environ.get('INFIELDS', None),
                            help='database fields to read (comma separated)')

    def setup(self):
        """ establish connection for example """

    @abstractmethod
    def read(self, key):
        """ return corresponding *fields* from database with key """

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    def close(self):
        pass


class DataWriter(ABC):
    kind = 'NONE'

    def __init__(self, config, logger=logger):
        self.config = config
        self.fields = self.config.out_fields.split(',')
        self.logger = logger

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--out-fields', type=str,
                            default=os.environ.get('OUTFIELDS', None),
                            help='database fields to write (comma separated)')

    def setup(self):
        """ establish connection for example """

    @abstractmethod
    def write(self, key, kv):
        """ return corresponding *fields* from database with key """

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    def close(self):
        pass


class MemoryReader(DataReader):
    """ MemoryReader read data from memory.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> data = {'key1':{'text':'text1', 'title':'title1'},'key2':{'text': 'text2', 'title': 'title2'}}
    >>> MemoryReader(SimpleNamespace(in_fields='title', mem=data)).read('key1')
    {'title': 'title1'}
    """
    kind = 'MEM'

    def read(self, key):
        dct = self.config.mem[key]
        return dict([(k, dct[k]) for k in self.fields])


class MemoryWriter(DataWriter):
    """ MemoryWriter writes data in memory
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> d = MemoryWriter(SimpleNamespace(out_fields='text,title'))
    >>> d.write('key1', {'text': 'text1', 'title': 'title1'})
    >>> d.write('key2', {'text': 'text2', 'title': 'title2'})
    >>> d.results
    {'key1': {'text': 'text1', 'title': 'title1'}, 'key2': {'text': 'text2', 'title': 'title2'}}
    """
    kind = 'MEM'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.results = {}

    def write(self, key, kvs):
        self.results[key] = kvs


class MySQLReader(DataReader):
    """ MySQLReader reads data from MySQL

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> MySQLReader.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text"])
    >>> with patch('mysql.connector.connect') as c:
    ...     MySQLReader(config, "text")
    MySQLReader(localhost:3306/database/table):['text']
    """
    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'MySQLReader({}/{}/{}):{}'.format(
            self.mysqlConfig.host,
            self.config.database,
            self.config.table,
            self.fields
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


class MySQLWriter(DataWriter):
    """ MySQLWriter writes data from MySQL

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> MySQLWriter.add_arguments(parser)
    >>> config = parser.parse_args(["--out-fields", "text"])
    >>> with patch('mysql.connector.connect') as c:
    ...     MySQLWriter(config, "text")
    MySQLWriter(localhost:3306/database/table):['text']
    """
    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'MySQLWriter({}/{}/{}):{}'.format(
            self.mysqlConfig.host,
            self.config.database,
            self.config.table,
            self.fields
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

    def write(self, key, kvs):
        query = "UPDATE {} SET {} WHERE {} = '{}'".format(
            self.config.table,
            ",".join(["{} = {}".format(k, v) for k, v in kvs.items()]),
            self.config.keyname,
            key
        )
        self.cursor.execute(query)
        self.db.commit()


class RedisReader(DataReader):
    """ RedisReader reads data from Redis

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> RedisReader.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text,title"])
    >>> with patch('mysql.connector.connect') as c:
    ...     RedisReader(config)
    RedisReader(localhost:6379):['text', 'title']
    """
    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'RedisReader({}:{}):{}'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.fields
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--redis', type=str,
            default=os.environ.get('REDIS', 'localhost:6379'),
            help='redis host:port'
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
        results = self.redis.mget(['{}:{}'.format(key, field) for field in self.fields])
        return dict(zip(self.fields, results))


class RedisWriter(DataWriter):
    """ RedisWriter reads data from Redis

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> RedisWriter.add_arguments(parser)
    >>> config = parser.parse_args(["--out-fields", "text,title"])
    >>> with patch('mysql.connector.connect') as c:
    ...     RedisWriter(config)
    RedisWriter(localhost:6379):['text', 'title']
    """
    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return 'RedisWriter({}:{}):{}'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.fields
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

    def write(self, key, kvs):
        """ entries are stored as following in redis:
            a set is managed for each key to contain fields available
            a key:field -> value for accessing field for each key

            TODO: check error after mset
        """
        self.redis.mset(
            dict(['{}:{}'.format(key, k) for k, v in kvs.items()]),
            ex=self.redisConfig.expire,
        )
