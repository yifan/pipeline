import logging
from logging import Logger
from typing import ClassVar, Iterator

from pydantic import Field, Json
from elasticsearch import Elasticsearch

from ..tap import SourceTap, BaseSourceSettings, DestinationTap, BaseDestinationSettings
from ..message import MessageBase


pipelineLogger = logging.getLogger("pipeline")


class ElasticSearchSourceSettings(BaseSourceSettings):
    uri: str = Field("http://localhost:9200", title="Elastic Search url")
    topic: str = Field(..., title="Elastic Search index")
    query: Json = Field('{"match_all": {}}', title="query as a json string")
    keyname: str = Field("_id", title="id field name, default is _id")
    size: int = Field(10, title="limit results in each query")


class ElasticSearchSource(SourceTap):
    """ElasticSearchSource read from ElasticSearch

    >>>
    >>>
    """

    settings: ElasticSearchSourceSettings
    elastic: Elasticsearch

    kind: ClassVar[str] = "ELASTIC"

    def __init__(
        self, settings: ElasticSearchSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.elastic = Elasticsearch([settings.uri])
        self.topic = settings.topic
        self.query = settings.query
        self.keyname = settings.keyname
        self.size = settings.size

    def __repr__(self) -> str:
        return f'ElasticSearchSource(host="{self.settings.uri}", topic="{self.topic}")'

    def __len__(self) -> int:
        return -1

    def read(self) -> Iterator[MessageBase]:
        res = self.elastic.search(
            index=self.topic,
            query=self.settings.query,
            size=self.size,
        )
        for hit in res["hits"]["hits"]:
            content = hit["_source"]
            _id = content[self.keyname] if self.keyname != "_id" else hit["_id"]
            yield MessageBase(id=_id, content=content)

    def acknowledge(self) -> None:
        """no acknowledge for Elastic Search"""
        pass

    def close(self) -> None:
        self.elastic.close()


class ElasticSearchDestinationSettings(BaseDestinationSettings):
    uri: str = Field("http://localhost:9200", title="ElasticSearch url")
    topic: str = Field(..., title="Elastic Search index")


class ElasticSearchDestination(DestinationTap):
    """ElasticSearchDestination writes to ElasticSearch"""

    settings: ElasticSearchDestinationSettings
    elastic: Elasticsearch

    kind: ClassVar[str] = "ELASTIC"

    def __init__(
        self,
        settings: ElasticSearchDestinationSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.elastic = Elasticsearch([settings.uri])
        self.elastic.indices.create(index=settings.topic, ignore=400)
        self.topic = settings.topic

    def __repr__(self) -> str:
        return f'ElasticSearchDestination(host="{self.settings.uri}", topic="{self.topic}")'

    def __len__(self) -> int:
        return -1

    def write(self, message: MessageBase) -> int:
        self.elastic.update(
            index=self.topic,
            id=message.id,
            body={"doc": message.content, "doc_as_upsert": True},
        )
        return 0

    def close(self) -> None:
        self.elastic.close()
