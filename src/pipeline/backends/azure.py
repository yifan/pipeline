import os

import azure.cosmosdb.table

from ..cache import Cache


class AzureTableCache(Cache):
    """AzureTableCache reads/writes data from/to Azure Table

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> AzureTableCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text,title"])
    >>> with patch('azure.cosmosdb.table.TableService') as c:
    ...     AzureTableCache(config, logger=logging)
    AzureTableCache:['text', 'title']:[]
    """

    kind = "AZURE"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return "AzureTableCache:{}:{}".format(self.in_fields, self.out_fields)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--azuredb",
            type=str,
            default=os.environ.get("AZUREDB", ""),
            help="azure table connection string",
        )
        parser.add_argument(
            "--table",
            type=str,
            default=os.environ.get("TABLE", ""),
            help="azure table table name",
        )

    def setup(self):
        self.service = azure.cosmosdb.table.TableService(
            endpoint_suffix="table.cosmos.azure.com",
            connection_string=self.config.azuredb,
        )

    def read(self, key):
        """entries are stored as following in redis:
        a set is managed for each key to contain fields available
        a key:field -> value for accessing field for each key

        TODO: raise error if fields are not available
        """
        entity = self.service.get_entity(
            self.config.table,
            "key",  # PartitionKey
            key,  # RowKey
            timeout=5.0,
        )
        if self.in_fields:
            fields = self.in_fields
        else:
            fields = [
                k
                for k in entity.keys()
                if k not in ("PartitionKey", "RowKey", "Timestamp", "etag")
            ]
        return {k: v for k, v in entity.items() if k in fields}

    def write(self, key, kvs):
        """entries are stored as following in redis:
        a set is managed for each key to contain fields available
        a key:field -> value for accessing field for each key
        """
        dct = {
            "PartitionKey": "key",
            "RowKey": key,
        }
        dct.update(kvs)
        self.service.insert_or_merge_entity(
            self.config.table,
            dct,
            timeout=5.0,
        )
