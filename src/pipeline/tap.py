#!/usr/bin/env python
import csv
import logging
import sys
import gzip
from abc import ABC, abstractmethod
from logging import Logger
from enum import Enum
from typing import Optional, Dict, Tuple, Iterator, List, ClassVar, Type, Union, Any

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

    def __init__(
        self, settings: SourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def read(self) -> Iterator[Message]:
        """ receive message. """
        raise NotImplementedError()

    def rewind(self) -> None:
        """ rewind to earliest message. """
        pass

    @classmethod
    def of(cls, kind: "TapKind") -> "SourceAndSettingsClasses":
        """
        >>> source = SourceTap.of(TapKind.MEM)
        """
        try:
            classesOrStrings, _ = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Source type '{kind}' is invalid") from None

        if isinstance(classesOrStrings, TapAndSettingsImportStrings):
            tapAndSettingsImportStrings = classesOrStrings
            return SourceAndSettingsClasses(
                sourceClass=import_class(tapAndSettingsImportStrings.tapClass),
                settingsClass=import_class(tapAndSettingsImportStrings.settingsClass),
            )
        else:
            return classesOrStrings

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

    def __init__(self, settings: DestinationSettings, logger: Logger = pipelineLogger):
        self.settings = settings
        self.topic = settings.topic
        self.logger = logger

    @abstractmethod
    def write(self, message: Message) -> int:
        """ send message. """
        raise NotImplementedError()

    @classmethod
    def of(cls, kind: "TapKind") -> "DestinationAndSettingsClasses":
        try:
            _, classesOrStrings = tap_kinds()[kind.value]
        except IndexError:
            raise TypeError(f"Destination type '{kind}' is invalid") from None

        if isinstance(classesOrStrings, TapAndSettingsImportStrings):
            tapAndSettingsImportStrings = classesOrStrings
            return DestinationAndSettingsClasses(
                destinationClass=import_class(tapAndSettingsImportStrings.tapClass),
                settingsClass=import_class(tapAndSettingsImportStrings.settingsClass),
            )
        else:
            return classesOrStrings

    def close(self) -> None:
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

    def read(self) -> Iterator[Message]:
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

    def __init__(
        self, settings: MemoryDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
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

    def read(self) -> Iterator[Message]:
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

    def read(self) -> Iterator[Message]:
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


class TapAndSettingsImportStrings(BaseModel):
    tapClass: str
    settingsClass: str


class SourceAndSettingsClasses(BaseModel):
    sourceClass: Type[SourceTap]
    settingsClass: Type[SourceSettings]


class DestinationAndSettingsClasses(BaseModel):
    destinationClass: Type[DestinationTap]
    settingsClass: Type[DestinationSettings]


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
                sourceClass=MemorySource, settingsClass=MemorySourceSettings
            ),
            DestinationAndSettingsClasses(
                destinationClass=MemoryDestination,
                settingsClass=MemoryDestinationSettings,
            ),
        ),
        "FILE": (
            SourceAndSettingsClasses(
                sourceClass=FileSource, settingsClass=FileSourceSettings
            ),
            DestinationAndSettingsClasses(
                destinationClass=FileDestination, settingsClass=FileDestinationSettings
            ),
        ),
        "CSV": (
            SourceAndSettingsClasses(
                sourceClass=CsvSource, settingsClass=CsvSourceSettings
            ),
            DestinationAndSettingsClasses(
                destinationClass=CsvDestination, settingsClass=CsvDestinationSettings
            ),
        ),
        "REDIS": (
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.redis:RedisStreamSource",
                settingsClass="pipeline.backends.redis:RedisSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.redis:RedisStreamDestination",
                settingsClass="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "LREDIS": (
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.redis:RedisListSource",
                settingsClass="pipeline.backends.redis:RedisSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.redis:RedisListDestination",
                settingsClass="pipeline.backends.redis:RedisDestinationSettings",
            ),
        ),
        "KAFKA": (
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.kafka:KafkaSource",
                settingsClass="pipeline.backends.kafka:KafkaSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.kafka:KafkaDestination",
                settingsClass="pipeline.backends.kafka:KafkaDestinationSettings",
            ),
        ),
        "PULSAR": (
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.pulsar:PulsarSource",
                settingsClass="pipeline.backends.pulsar:PulsarSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.pulsar:PulsarDestination",
                settingsClass="pipeline.backends.pulsar:PulsarDestinationSettings",
            ),
        ),
        "RABBITMQ": (
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.rabbitmq:RabbitMQSource",
                settingsClass="pipeline.backends.rabbitmq:RabbitMQSourceSettings",
            ),
            TapAndSettingsImportStrings(
                tapClass="pipeline.backends.rabbitmq:RabbitMQDestination",
                settingsClass="pipeline.backends.rabbitmq:RabbitMQDestinationSettings",
            ),
        ),
    }


TapKind = Enum("TapKind", {x: x for x in tap_kinds().keys()})  # type: ignore
