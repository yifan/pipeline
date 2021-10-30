import logging
from logging import Logger
from typing import ClassVar, Iterator

from pydantic import Field, Json
from elasticsearch import Elasticsearch

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


class ElasticSearchSourceSettings(SourceSettings):
    uri: str = Field("http://localhost:9200", title="Elastic Search url")
    topic: str = Field("", title="Elastic Search index")
    query: Json = Field('{"match_all": {}}', title="query as a json string")
    keyname: str = Field("id", title="id field name")
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

    def read(self) -> Iterator[MessageBase]:
        res = self.elastic.search(
            index=self.topic,
            query=self.settings.query,
            size=self.size,
        )
        for hit in res["hits"]["hits"]:
            content = hit["_source"]
            yield MessageBase(id=content[self.keyname], content=content)

    def acknowledge(self) -> None:
        """no acknowledge for Elastic Search"""
        pass

    def close(self) -> None:
        self.elastic.close()


class ElasticSearchDestinationSettings(DestinationSettings):
    uri: str = Field("http://localhost:9200", title="ElasticSearch url")
    topic: str = Field("", title="Elastic Search index")


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

    def write(self, message: MessageBase) -> int:
        self.elastic.update(
            index=self.topic,
            id=message.id,
            body={"doc": message.content, "doc_as_upsert": True},
        )
        return 0

    def close(self) -> None:
        self.elastic.close()
