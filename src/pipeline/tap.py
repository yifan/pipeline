#!/usr/bin/env python
import csv
import logging
import sys
import gzip
import json
from abc import ABC, abstractmethod
from logging import Logger
from enum import Enum
from typing import Optional, Dict, Tuple, Iterator, List, ClassVar, Type, Union, Any

from pydantic import BaseModel, Field

from .helpers import Settings
from .message import MessageBase, Message, deserialize_message, serialize_message
from .importor import import_class


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


class SourceSettings(Settings):
    namespace: str = Field(None, title="source namespace")
    topic: str = Field("in-topic", title="source topic")
    timeout: int = Field(0, title="seconds to time out")

    class Config:
        env_prefix = "in_"


class SourceTap(ABC):
    """Tap defines the interface for connecting components in pipeline.
    A source will emit Message.
    """

    def __init__(
        self, settings: SourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def read(self) -> Iterator[MessageBase]:
        """receive message."""
        raise NotImplementedError()

    def rewind(self) -> None:
        """rewind to earliest message."""
        raise NotImplementedError("rewind is not implemented")

    def length(self) -> None:
        """return how many messages in source"""
        raise NotImplementedError("length is not implemented")

    @classmethod
    def of(cls, kind: "TapKind") -> "SourceAndSettingsClasses":
        """
        >>> source = SourceTap.of(TapKind.MEM)
        """
        try:
            classes_or_strings, _ = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Source type '{kind}' is invalid") from None

        if isinstance(classes_or_strings, TapAndSettingsImportStrings):
            import_strings = classes_or_strings
            return SourceAndSettingsClasses(
                source_class=import_class(import_strings.tap_class),
                settings_class=import_class(import_strings.settings_class),
            )
        else:
            return classes_or_strings

    def close(self) -> None:
        """implementation needed when subclassing"""
        pass

    def acknowledge(self) -> None:
        """implementation needed when subclassing"""
        pass


class DestinationSettings(Settings):
    namespace: str = Field(None, title="destination namespace")
    topic: str = Field("out-topic", title="output topic")
    compress: bool = Field(False, title="turn on compression")

    class Config:
        env_prefix = "out_"


class DestinationTap(ABC):
    """Tap defines the interface for connecting components in pipeline."""

    def __init__(self, settings: DestinationSettings, logger: Logger = pipelineLogger):
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def write(self, message: MessageBase) -> int:
        """send message."""
        raise NotImplementedError()

    def length(self) -> None:
        """return how many messages in backlog"""
        raise NotImplementedError("length is not implemented")

    @classmethod
    def of(cls, kind: "TapKind") -> "DestinationAndSettingsClasses":
        try:
            _, classes_or_strings = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Destination type '{kind}' is invalid") from None

        if isinstance(classes_or_strings, TapAndSettingsImportStrings):
            import_strings = classes_or_strings
            return DestinationAndSettingsClasses(
                destination_class=import_class(import_strings.tap_class),
                settings_class=import_class(import_strings.settings_class),
            )
        else:
            return classes_or_strings

    def close(self) -> None:
        """implementation needed when subclassing"""
        pass


class MemorySourceSettings(SourceSettings):
    topic: str = "in-topic"
    data: List[Dict[str, Any]] = []


class MemorySource(SourceTap):
    """MemorySource iterates over a list of dict from 'data' in config.
    It is for testing only.

    >>> data = [{'id':1},{'id':2}]
    >>> settings = MemorySourceSettings(data=data)
    >>> [ m.content for m in MemorySource(settings=settings).read() ]
    [{'id': 1}, {'id': 2}]
    """

    kind = "MEM"

    def __init__(
        self, settings: MemorySourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.data = settings.data
        self.storage = [
            Message(content=content).serialize() for content in settings.data
        ]

    def load_data(self, data):
        self.data = data
        self.storage = [Message(content=content).serialize() for content in data]

    def read(self) -> Iterator[MessageBase]:
        for serialized in self.storage:
            yield deserialize_message(serialized)

    def rewind(self) -> None:
        raise NotImplementedError()


class MemoryDestinationSettings(DestinationSettings):
    topic: str = "out-topic"


class MemoryDestination(DestinationTap):
    """MemoryDestination stores dicts written in results.
    It is for testing only.

    >>> d = MemoryDestination(MemoryDestinationSettings())
    >>> d.write(Message(content={"id": 1}))
    0
    >>> d.write(Message(content={"id": 2}))
    0
    >>> [r.content for r in d.results]
    [{'id': 1}, {'id': 2}]
    """

    kind: ClassVar[str] = "MEM"

    def __init__(
        self, settings: MemoryDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings=settings, logger=logger)
        self.results: List[MessageBase] = []
        self.storage: List[bytes] = []

    def write(self, message: MessageBase) -> int:
        self.results.append(message)
        self.storage.append(serialize_message(message))
        return 0


class FileSourceSettings(SourceSettings):
    filename: str = Field(
        None, title="input filename, use '-' for stdin", required=True
    )
    content_only: bool = Field(False, title="input contains only content for messages")


class FileSource(SourceTap):
    """FileSource iterates over lines from a input
    text file (utf-8), each line should be a json string for a dict.
    It can be used for integration test for workers.

    >>> import tempfile
    >>> with tempfile.NamedTemporaryFile() as tmpfile:
    ...     tmpfile.write(Message(content={"id": 0}).serialize()) and True
    ...     tmpfile.flush()
    ...     settings = FileSourceSettings(filename=tmpfile.name)
    ...     fileSource = FileSource(settings)
    ...     [m.content["id"] for m in fileSource.read()]
    True
    [0]
    """

    kind = "FILE"

    def __init__(
        self, settings: FileSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings=settings, logger=logger)
        self.filename = settings.filename
        if self.filename == "-":
            self.infile = sys.stdin.buffer
        elif self.filename.endswith(".gz"):
            self.infile = gzip.open(self.filename)  # type: ignore
        else:
            self.infile = open(self.filename, "rb")
        logger.info("File Source: %s", self.filename)

    def __repr__(self) -> str:
        return 'FileSource("{}")'.format(self.filename)

    def read(self) -> Iterator[MessageBase]:
        if self.settings.content_only:
            for line in self.infile:
                content = json.loads(line)
                if content.get("id"):
                    yield Message(content=content, id=content.get("id"))
                else:
                    yield Message(content=content)
        else:
            for line in self.infile:
                yield deserialize_message(line)


class FileDestinationSettings(DestinationSettings):
    filename: str = Field(None, title="output filename", required=True)
    overwrite: bool = Field(False, title="overwrite output file if exists")
    content_only: bool = Field(False, title="output content only, no messages")


class FileDestination(DestinationTap):
    """FileDestination writes items to an output file, one item per line in json format.

    >>> import os, tempfile
    >>> FileDestination(FileDestinationSettings(filename="/tmp/out-topic.json"))
    FileDestination("/tmp/out-topic.json")
    """

    kind = "FILE"

    def __init__(
        self, settings: FileDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.filename = settings.filename
        if self.filename == "-":
            self.outFile = sys.stdout.buffer
        elif self.filename.endswith(".gz"):
            if settings.overwrite:
                self.outFile = gzip.GzipFile(self.filename, "wb")  # type: ignore
            else:
                self.outFile = gzip.GzipFile(self.filename, "ab")  # type: ignore
        else:
            if settings.overwrite:
                self.outFile = open(self.filename, "wb")
            else:
                self.outFile = open(self.filename, "ab")
        self.logger.info("File Destination: %s", self.filename)

    def __repr__(self) -> str:
        return 'FileDestination("{}")'.format(self.filename)

    def write(self, message: MessageBase) -> int:
        if self.settings.content_only:
            serialized = (json.dumps(message.content) + "\n").encode("utf-8")
        else:
            serialized = message.serialize(compress=False) + "\n".encode("utf-8")
        self.outFile.write(serialized)
        return len(serialized)

    def close(self) -> None:
        if self.filename != "-":
            self.outFile.close()
            self.logger.info("File Destination closed")


class DialectEnum(str, Enum):
    Excel = "excel"
    ExcelTab = "excel-tab"
    Unix = "unix"


class CsvSourceSettings(FileSourceSettings):
    dialect: DialectEnum = Field(
        DialectEnum.Excel, title="csv format: excel or excel-tab"
    )


class CsvSource(SourceTap):
    """CsvSource iterates over csv file.

    >>> import tempfile, csv
    >>> from argparse import ArgumentParser
    >>> with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
    ...     fieldnames = ['id', 'field1', 'field2']
    ...     writer = csv.DictWriter(tmpfile, fieldnames=fieldnames)
    ...     writer.writeheader() #doctest:+SKIP
    ...     writer.writerow({'id': 0, 'field1': 'value1', 'field2': 'value2'}) #doctest:+SKIP
    ...     tmpfile.flush()
    ...     config = parser.parse_args("--infile {}".format(tmpfile.name).split())
    ...     csvSource = CsvSource(settings=CsvSourceSettings(filename=tmpfile.name))
    ...     [m.dct["id"] for m in csvSource.read()]
    ['0']
    """

    kind = "CSV"

    def __init__(
        self, settings: CsvSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.filename = settings.filename
        if self.filename == "-":
            self.infile = sys.stdin
        else:
            self.infile = open(self.filename, "r")
        self.reader = csv.DictReader(self.infile, dialect=settings.dialect)
        self.logger.info("CSV Source: %s", self.filename)

    def __repr__(self) -> str:
        return 'CsvSource("{}")'.format(self.filename)

    def read(self) -> Iterator[MessageBase]:
        for row in self.reader:
            yield Message(content=row)


class CsvDestinationSettings(FileDestinationSettings):
    dialect: DialectEnum = Field(
        DialectEnum.Excel, title="csv format: excel or excel-tab"
    )


class CsvDestination(DestinationTap):
    """CsvDestination writes items to a csv file.

    >>> import os, tempfile, csv
    >>> tmpdir = tempfile.mkdtemp()
    >>> outFilename = os.path.join(tmpdir, 'outfile.csv')
    >>> CsvDestination(settings=CsvDestinationSettings(filename=outFilename))
    CsvDestination("...outfile.csv")
    """

    kind = "CSV"

    def __init__(
        self, settings: CsvDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.dialect = settings.dialect
        self.filename = settings.filename

        if self.filename == "-":
            self.outFile = sys.stdout
        else:
            if settings.overwrite:
                self.outFile = open(self.filename, "w")
            else:
                self.outFile = open(self.filename, "a")
        self.writer: Optional[csv.DictWriter] = None
        self.logger.info("CsvDestination: %s", self.filename)

    def __repr__(self) -> str:
        return 'CsvDestination("{}")'.format(self.filename)

    def write(self, message: MessageBase) -> int:
        if self.writer is None:
            self.writer = csv.DictWriter(
                self.outFile,
                fieldnames=message.content.keys(),
                dialect=self.dialect,
            )
            self.writer.writeheader()
        self.writer.writerow(message.content)
        self.outFile.flush()
        return 0

    def close(self) -> None:
        if self.filename != "-":
            self.outFile.close()
            self.logger.info("CsvDestination closed")


class TapAndSettingsImportStrings(BaseModel):
    tap_class: str
    settings_class: str


class SourceAndSettingsClasses(BaseModel):
    source_class: Type[SourceTap]
    settings_class: Type[SourceSettings]


class DestinationAndSettingsClasses(BaseModel):
    destination_class: Type[DestinationTap]
    settings_class: Type[DestinationSettings]


def tap_kinds() -> Dict[
    str,
    Union[
        Tuple[SourceAndSettingsClasses, DestinationAndSettingsClasses],
        Tuple[TapAndSettingsImportStrings, TapAndSettingsImportStrings],
    ],
]:
    return {
        "MEM": (
            SourceAndSettingsClasses(
                source_class=MemorySource, settings_class=MemorySourceSettings
            ),
            DestinationAndSettingsClasses(
                destination_class=MemoryDestination,
                settings_class=MemoryDestinationSettings,
            ),
        ),
        "FILE": (
            SourceAndSettingsClasses(
                source_class=FileSource, settings_class=FileSourceSettings
            ),
            DestinationAndSettingsClasses(
                destination_class=FileDestination,
                settings_class=FileDestinationSettings,
            ),
        ),
        "CSV": (
            SourceAndSettingsClasses(
                source_class=CsvSource, settings_class=CsvSourceSettings
            ),
            DestinationAndSettingsClasses(
                destination_class=CsvDestination, settings_class=CsvDestinationSettings
            ),
        ),
        "XREDIS": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.redis:RedisStreamSource",
                settings_class="pipeline.backends.redis:RedisSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.redis:RedisStreamDestination",
                settings_class="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "LREDIS": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.redis:RedisListSource",
                settings_class="pipeline.backends.redis:RedisSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.redis:RedisListDestination",
                settings_class="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "KAFKA": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.kafka:KafkaSource",
                settings_class="pipeline.backends.kafka:KafkaSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.kafka:KafkaDestination",
                settings_class="pipeline.backends.kafka:KafkaDestinationSettings",
            ),
        ),
        "PULSAR": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.pulsar:PulsarSource",
                settings_class="pipeline.backends.pulsar:PulsarSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.pulsar:PulsarDestination",
                settings_class="pipeline.backends.pulsar:PulsarDestinationSettings",
            ),
        ),
        "RABBITMQ": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.rabbitmq:RabbitMQSource",
                settings_class="pipeline.backends.rabbitmq:RabbitMQSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.rabbitmq:RabbitMQDestination",
                settings_class="pipeline.backends.rabbitmq:RabbitMQDestinationSettings",
            ),
        ),
        "ELASTIC": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.elasticsearch:ElasticSearchSource",
                settings_class="pipeline.backends.elasticsearch:ElasticSearchSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.elasticsearch:ElasticSearchDestination",
                settings_class="pipeline.backends.elasticsearch:ElasticSearchDestinationSettings",
            ),
        ),
        "MONGO": (
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.mongodb:MongodbSource",
                settings_class="pipeline.backends.mongodb:MongodbSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tap_class="pipeline.backends.mongodb:MongodbDestination",
                settings_class="pipeline.backends.mongodb:MongodbDestinationSettings",
            ),
        ),
    }


TapKind = Enum("TapKind", {x: x for x in tap_kinds().keys()})  # type: ignore
