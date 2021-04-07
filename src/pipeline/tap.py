#!/usr/bin/env python
import csv
import logging
import sys
import gzip
from abc import ABC, abstractmethod

from enum import Enum
from typing import Dict, Tuple, Generator, List, ClassVar, Type, Union
from pydantic import BaseModel, Field

from .helpers import Settings
from .message import Message
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

    def __init__(self, settings: SourceSettings, logger=pipelineLogger):
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def read(self) -> Generator[Message, None, None]:
        """ receive message. """
        raise NotImplementedError()

    def rewind(self) -> None:
        """ rewind to earliest message. """
        pass

    @classmethod
    def of(cls, kind: "TapKind") -> "SourceAndSettings":
        """
        >>> source = SourceTap.of(TapKind.MEM)
        """
        try:
            sourceAndSettings, _ = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Source type '{kind}' is invalid") from None

        if isinstance(sourceAndSettings.sourceClass, str):
            return SourceAndSettings(
                sourceClass=import_class(sourceAndSettings.sourceClass),
                settings=import_class(sourceAndSettings.settings),
            )
        else:
            return sourceAndSettings

    def close(self) -> None:
        pass

    def acknowledge(self) -> None:
        pass


class DestinationSettings(Settings):
    namespace: str = Field(None, title="destination namespace")
    topic: str = Field("out-topic", title="output topic")
    compress: bool = Field(False, title="turn on compression")

    class Config:
        env_prefix = "out_"


class DestinationTap(ABC):
    """Tap defines the interface for connecting components in pipeline."""

    def __init__(self, settings: DestinationSettings, logger=pipelineLogger):
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def write(self, message: Message) -> int:
        """ send message. """
        raise NotImplementedError()

    @classmethod
    def of(cls, kind: "TapKind") -> "DestinationAndSettings":
        try:
            _, destinationClassAndSettings = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Destination type '{kind}' is invalid") from None

        if isinstance(destinationClassAndSettings.destinationClass, str):
            return DestinationAndSettings(
                destinationClass=import_class(
                    destinationClassAndSettings.destinationClass
                ),
                settings=import_class(destinationClassAndSettings.settings),
            )
        else:
            return destinationClassAndSettings

    def close(self) -> None:
        pass


class MemorySourceSettings(SourceSettings):
    topic: str = "in-topic"
    data: List[dict] = []


class MemorySource(SourceTap):
    """MemorySource iterates over a list of dict from 'data' in config.
    It is for testing only.

    >>> data = [{'id':1},{'id':2}]
    >>> settings = MemorySourceSettings(data=data)
    >>> [ m.content for m in MemorySource(settings=settings).read() ]
    [{'id': 1}, {'id': 2}]
    """

    kind = "MEM"

    def __init__(self, settings: MemorySourceSettings, logger=pipelineLogger):
        super().__init__(settings, logger)
        self.data = settings.data

    def read(self) -> Generator[Message, None, None]:
        for i in self.data:
            yield Message(content=i)

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

    def __init__(self, settings: MemoryDestinationSettings, logger=pipelineLogger):
        super().__init__(settings=settings, logger=logger)
        self.results: List[Message] = []

    def write(self, message: Message) -> int:
        self.results.append(message)
        return 0


class FileSourceSettings(SourceSettings):
    filename: str = Field(
        None, title="input filename, use '-' for stdin", required=True
    )


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

    def __init__(self, settings: FileSourceSettings, logger=pipelineLogger) -> None:
        super().__init__(settings=settings, logger=logger)
        self.filename = settings.filename
        if self.filename == "-":
            self.infile = sys.stdin.buffer
        elif self.filename.endswith(".gz"):
            self.infile = gzip.open(self.filename)
        else:
            self.infile = open(self.filename, "rb")
        logger.info("File Source: %s", self.filename)

    def __repr__(self) -> str:
        return 'FileSource("{}")'.format(self.filename)

    def read(self) -> Generator[Message, None, None]:
        for line in self.infile:
            yield Message.deserialize(line)


class FileDestinationSettings(DestinationSettings):
    filename: str = Field(None, title="output filename", required=True)
    overwrite: bool = Field(False, title="overwrite output file if exists")


class FileDestination(DestinationTap):
    """FileDestination writes items to an output file, one item per line in json format.

    >>> import os, tempfile
    >>> FileDestination(FileDestinationSettings(filename="out-topic.json"))
    FileDestination("out-topic.json")
    """

    kind = "FILE"

    def __init__(self, settings: FileDestinationSettings, logger=pipelineLogger):
        super().__init__(settings, logger)
        self.filename = settings.filename
        if self.filename == "-":
            self.outFile = sys.stdout.buffer
        elif self.filename.endswith(".gz"):
            if settings.overwrite:
                self.outFile = gzip.GzipFile(self.filename, "wb")
            else:
                self.outFile = gzip.GzipFile(self.filename, "ab")
        else:
            if settings.overwrite:
                self.outFile = open(self.filename, "wb")
            else:
                self.outFile = open(self.filename, "ab")
        self.logger.info("File Destination: %s", self.filename)

    def __repr__(self):
        return 'FileDestination("{}")'.format(self.filename)

    def write(self, message: Message) -> int:
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

    def __init__(self, settings: CsvSourceSettings, logger=pipelineLogger):
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

    def read(self) -> Generator[Message, None, None]:
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

    def __init__(self, settings: CsvDestinationSettings, logger=pipelineLogger):
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
        self.writer = None
        self.logger.info("CsvDestination: %s", self.filename)

    def __repr__(self):
        return 'CsvDestination("{}")'.format(self.filename)

    def write(self, message: Message) -> int:
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


class SourceAndSettings(BaseModel):
    sourceClass: Union[Type[SourceTap], str]
    settings: Union[Type[SourceSettings], str]


class DestinationAndSettings(BaseModel):
    destinationClass: Union[Type[DestinationTap], str]
    settings: Union[Type[DestinationSettings], str]


def tap_kinds() -> Dict[str, Tuple]:
    return {
        "MEM": (
            SourceAndSettings(sourceClass=MemorySource, settings=MemorySourceSettings),
            DestinationAndSettings(
                destinationClass=MemoryDestination, settings=MemoryDestinationSettings
            ),
        ),
        "FILE": (
            SourceAndSettings(sourceClass=FileSource, settings=FileSourceSettings),
            DestinationAndSettings(
                destinationClass=FileDestination, settings=FileDestinationSettings
            ),
        ),
        "CSV": (
            SourceAndSettings(sourceClass=CsvSource, settings=CsvSourceSettings),
            DestinationAndSettings(
                destinationClass=CsvDestination, settings=CsvDestinationSettings
            ),
        ),
        "REDIS": (
            SourceAndSettings(
                sourceClass="pipeline.backends.redis:RedisStreamSource",
                settings="pipeline.backends.redis:RedisSourceSettings",
            ),
            DestinationAndSettings(
                destinationClass="pipeline.backends.redis:RedisStreamDestination",
                settings="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "LREDIS": (
            SourceAndSettings(
                sourceClass="pipeline.backends.redis:RedisListSource",
                settings="pipeline.backends.redis:RedisSourceSettings",
            ),
            DestinationAndSettings(
                destinationClass="pipeline.backends.redis:RedisListDestination",
                settings="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "KAFKA": (
            SourceAndSettings(
                sourceClass="pipeline.backends.kafka:KafkaSource",
                settings="pipeline.backends.kafka:KafkaSourceSettings",
            ),
            DestinationAndSettings(
                destinationClass="pipeline.backends.kafka:KafkaDestination",
                settings="pipeline.backends.kafka:KafkaDestinationSettings",
            ),
        ),
        "PULSAR": (
            SourceAndSettings(
                sourceClass="pipeline.backends.pulsar:PulsarSource",
                settings="pipeline.backends.pulsar:PulsarSourceSettings",
            ),
            DestinationAndSettings(
                destinationClass="pipeline.backends.pulsar:PulsarDestination",
                settings="pipeline.backends.pulsar:PulsarDestinationSettings",
            ),
        ),
        "RABBITMQ": (
            SourceAndSettings(
                sourceClass="pipeline.backends.rabbitmq:RabbitMQSource",
                settings="pipeline.backends.rabbitmq:RabbitMQSourceSettings",
            ),
            DestinationAndSettings(
                destinationClass="pipeline.backends.rabbitmq:RabbitMQDestination",
                settings="pipeline.backends.rabbitmq:RabbitMQDestinationSettings",
            ),
        ),
    }


TapKind = Enum("TapKind", {x: x for x in tap_kinds().keys()})
