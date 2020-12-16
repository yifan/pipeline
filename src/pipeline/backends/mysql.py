import os

import mysql.connector as mysql

from ..cache import parse_connection_string, Cache
from ..exception import PipelineError


class MySqlCache(Cache):
    """MySQLCache reads/writes data from/to MySQL

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> MySqlCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text"])
    >>> with patch('mysql.connector.connect') as c:
    ...     MySqlCache(config, logger=logging)
    MySqlCache(localhost:3306/database/table):['text']:[]
    """

    kind = "MYSQL"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return "MySqlCache({}/{}/{}):{}:{}".format(
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
            "--mysql",
            type=str,
            default=os.environ.get("MYSQL", "localhost:3306"),
            help="mysql database host:port",
        )
        parser.add_argument(
            "--database",
            type=str,
            default=os.environ.get("MYSQLDATABASE", "database"),
            help="muysql database",
        )
        parser.add_argument(
            "--table",
            type=str,
            default=os.environ.get("MYSQLTABLE", "table"),
            help="mysql database table",
        )
        parser.add_argument(
            "--keyname",
            type=str,
            default=os.environ.get("KEYNAME", "id"),
            help="database field name of key",
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
            self.config.in_fields, self.config.table, self.config.keyname, key
        )
        self.cursor.execute(query)
        r = self.cursor.fetchone()
        if r is None:
            raise PipelineError("Data with key is not found")
        return dict(zip(self.config.in_fields.split(","), r))

    def write(self, key, kvs):
        query = "UPDATE {} SET {} WHERE {} = '{}'".format(
            self.config.table,
            ",".join(["{} = {}".format(k, v) for k, v in kvs.items()]),
            self.config.keyname,
            key,
        )
        self.cursor.execute(query)
        self.db.commit()
