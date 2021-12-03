import logging
from logging import Logger
from typing import ClassVar, Iterator

from pydantic import Field, Json
from pymongo import MongoClient
from pymongo.collection import Collection


from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


class MongodbSourceSettings(SourceSettings):
    uri: str = Field("mongodb://localhost:27017", title="MongoDB url")
    database: str = Field("", title="MongoDB database")
    topic: str = Field("", title="collection name")
    keyname: str = Field("id", title="keyname")
    query: Json = Field("{}", title="query as json string")


class MongodbSource(SourceTap):
    """ """

    settings: MongodbSourceSettings
    client: MongoClient

    kind: ClassVar[str] = "MONGODB"

    def __init__(
        self, settings: MongodbSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.keyname = settings.keyname
        self.query = settings.query
        self.client = MongoClient(settings.uri)
        database = MongoClient(settings.uri).get_database(settings.database)
        self.collection = Collection(
            database,
            settings.topic,
        )

    def __repr__(self) -> str:
        return f'MongodbSource(host="{self.settings.uri}", topic="{self.topic}")'

    def read(self) -> Iterator[MessageBase]:
        for content in self.collection.find(self.query):
            yield MessageBase(
                id=content.get(self.keyname),
                content={k: v for k, v in content.items() if k != "_id"},
            )

    def acknowledge(self) -> None:
        """no acknowledge for Elastic Search"""
        pass

    def close(self) -> None:
        self.client.close()


class MongodbDestinationSettings(DestinationSettings):
    uri: str = Field("mongodb://localhost:27017", title="MongoDB url")
    database: str = Field("", title="MongoDB database")
    topic: str = Field("", title="collection name")
    keyname: str = Field("id", title="keyname")


class MongodbDestination(DestinationTap):
    """ """

    settings: MongodbDestinationSettings
    client: MongoClient

    kind: ClassVar[str] = "MONGODB"

    def __init__(
        self, settings: MongodbDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        keynames = settings.keyname.split(",")
        if len(keynames) > 1:
            self.keynames = keynames
            self.get_filter = self.get_filter_keynames
        else:
            self.keyname = settings.keyname
            self.get_filter = self.get_filter_keyname
        self.client = MongoClient(settings.uri)
        database = MongoClient(settings.uri).get_database(settings.database)
        self.collection = Collection(
            database,
            settings.topic,
        )

    def __repr__(self) -> str:
        return f'MongdbDestination(host="{self.settings.uri}", topic="{self.topic}")'

    def get_filter_keyname(self, dct):
        return {self.keyname: dct.get(self.keyname)}

    def get_filter_keynames(self, dct):
        return {k: dct.get(k) for k in self.keynames}

    def write(self, message: MessageBase) -> int:
        self.collection.update_one(
            self.get_filter(message.content),
            {"$set": message.content},
            upsert=True,
        )

    def close(self) -> None:
        self.client.close()
