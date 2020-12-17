#!/usr/bin/env python
import csv
import logging
import os
import sys
from abc import ABC, abstractmethod

from .message import Message
from .importor import import_class


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


def supportedTapKinds():
    return {
        "MEM": (MemorySource, MemoryDestination),
        "FILE": (FileSource, FileDestination),
        "CSV": (CsvSource, CsvDestination),
        "REDIS": (
            "pipeline.backends.redis:RedisStreamSource",
            "pipeline.backends.redis:RedisStreamDestination",
        ),
        "LREDIS": (
            "pipeline.backends.redis:RedisListSource",
            "pipeline.backends.redis:RedisListDestination",
        ),
        "KAFKA": (
            "pipeline.backends.kafka:KafkaSource",
            "pipeline.backends.kafka:KafkaDestination",
        ),
        "PULSAR": (
            "pipeline.backends.pulsar:PulsarSource",
            "pipeline.backends.pulsar:PulsarDestination",
        ),
        "RABBITMQ": (
            "pipeline.backends.rabbitmq:RabbitMQSource",
            "pipeline.backends.rabbitmq:RabbitMQDestination",
        ),
    }


def KindsOfSource():
    return supportedTapKinds().keys()


def SourceOf(typename):
    try:
        sourceClass, _ = supportedTapKinds()[typename]
    except IndexError:
        raise TypeError(f"Source type '{typename}' is invalid") from None

    if isinstance(sourceClass, str):
        return import_class(sourceClass)
    else:
        return sourceClass


def DestinationOf(typename):
    try:
        _, destinationClass = supportedTapKinds()[typename]
    except IndexError:
        raise TypeError(f"Destination type '{typename}' is invalid") from None

    if isinstance(destinationClass, str):
        return import_class(destinationClass)
    else:
        return destinationClass


class SourceTap(ABC):
    """Tap defines the interface for connecting components in pipeline.
    A source will emit Message.
    """

    kind = "NONE"

    def __init__(self, config, logger=pipelineLogger):
        self.config = config
        self.rewind = config.rewind
        self.topic = config.in_topic
        self.timeout = config.timeout
        self.logger = logger
        self.messageClass = config.message if hasattr(config, "message") else Message

    @abstractmethod
    def read(self):
        """ receive message. """
        raise NotImplementedError()

    def rewind(self):
        """ rewind to earliest message. """
        pass

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "--namespace",
            type=str,
            default=os.environ.get("NAMESPACE"),
            help="namespace (default: None)",
        )
        parser.add_argument(
            "--in-topic",
            type=str,
            default=os.environ.get("INTOPIC", "in-topic"),
            help="topic to read from",
        )
        parser.add_argument(
            "--rewind",
            action="store_true",
            default=False,
            help="read from earliest message",
        )
        parser.add_argument(
            "--timeout",
            type=int,
            default=os.environ.get("TIMEOUT", 0),
            help="request timeout",
        )

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    def close(self):
        pass

    def acknowledge(self):
        pass


class DestinationTap(ABC):
    """Tap defines the interface for connecting components in pipeline."""

    kind = "NONE"

    def __init__(self, config, logger=pipelineLogger):
        self.config = config
        self.topic = config.out_topic
        self.logger = logger
        self.messageClass = config.message if hasattr(config, "message") else Message

    @abstractmethod
    def write(self, message):
        """ send message. """
        raise NotImplementedError()

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "--namespace",
            type=str,
            default=os.environ.get("NAMESPACE"),
            help="namespace (default: None)",
        )
        parser.add_argument(
            "--out-topic",
            type=str,
            default=os.environ.get("OUTTOPIC", "out-topic"),
            help="topic to write",
        )

    def close(self):
        pass


class MemorySource(SourceTap):
    """MemorySource iterates over a list of dict from 'data' in config.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> data = [{'id':1},{'id':2}]
    >>> config = SimpleNamespace(rewind=False, in_topic='test', data=data, timeout=0)
    >>> [ m.dct for m in MemorySource(config).read() ]
    [{'id': 1}, {'id': 2}]
    """

    kind = "MEM"

    def read(self):
        for i in self.config.data:
            yield self.messageClass(i, config=self.config)

    def rewind(self):
        pass


class MemoryDestination(DestinationTap):
    """MemoryDestination stores dicts written in results.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> d = MemoryDestination(SimpleNamespace(out_topic='test'))
    >>> d.write(Message({"id": 1}))
    >>> d.write(Message({"id": 2}))
    >>> [r.dct for r in d.results]
    [{'id': 1}, {'id': 2}]
    """

    kind = "MEM"

    def __init__(self, config, logger=pipelineLogger):
        super().__init__(config, logger)
        self.results = []

    def write(self, message):
        self.results.append(message)


class FileSource(SourceTap):
    """FileSource iterates over lines from a input
    text file (utf-8), each line should be a json string for a dict.
    It can be used for integration test for workers.

    >>> import tempfile
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileSource.add_arguments(parser)
    >>> with tempfile.NamedTemporaryFile() as tmpfile:
    ...     tmpfile.write(b'[{ }, {"id": 0}]') and True
    ...     tmpfile.flush()
    ...     config = parser.parse_args("--infile {}".format(tmpfile.name).split())
    ...     fileSource = FileSource(config)
    ...     [m.dct["id"] for m in fileSource.read()]
    True
    [0]
    """

    kind = "FILE"

    def __init__(self, config, logger=pipelineLogger):
        super().__init__(config, logger)
        if config.infile == "-":
            self.infile = sys.stdin
        else:
            self.infile = open(config.infile, "rb")
        self.repeat = config.repeat
        self.logger.info("File Source: %s (repeat %d)", config.infile, config.repeat)

    def __repr__(self):
        return 'FileSource("{}")'.format(self.infile.name)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--infile",
            type=str,
            required=True,
            help="input file containing one message in JSON format per line",
        )
        parser.add_argument(
            "--repeat", type=int, default=1, help="repeat input N times"
        )

    def read(self):
        for i in range(0, self.repeat):
            for line in self.infile:
                yield self.messageClass.deserialize(line, config=self.config)
            if self.repeat > 1:
                self.infile.seek(0, 0)


class FileDestination(DestinationTap):
    """FileDestination writes items to an output file, one item per line in json format.

    >>> import os, tempfile
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileDestination.add_arguments(parser)
    >>> tmpdir = tempfile.mkdtemp()
    >>> outFilename = os.path.join(tmpdir, 'outfile.txt')
    >>> config = parser.parse_args(args=[])
    >>> FileDestination(config)
    FileDestination("out-topic.json")
    >>> config = parser.parse_args("--outfile {}".format(outFilename).split())
    >>> FileDestination(config)
    FileDestination("...outfile.txt")
    """

    kind = "FILE"

    def __init__(self, config, logger=pipelineLogger):
        super().__init__(config, logger)
        if config.outfile is None:
            if config.out_topic.find(".") >= 0:
                self.filename = config.out_topic
            else:
                self.filename = config.out_topic + ".json"
        else:
            self.filename = config.outfile

        if config.outfile == "-":
            self.outFile = sys.stdout
        else:
            if config.overwrite:
                self.outFile = open(self.filename, "w")
            else:
                self.outFile = open(self.filename, "a")
        self.logger.info("File Destination: %s", self.filename)

    def __repr__(self):
        return 'FileDestination("{}")'.format(self.filename)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument("--outfile", type=str, help="output json file")
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help="overwrite output file instead of append",
        )

    def write(self, message):
        serialized = message.serialize()
        print(serialized, file=self.outFile, flush=True)
        return len(serialized)

    def close(self):
        if self.filename != "-":
            self.outFile.close()
            self.logger.info("File Destination closed")


class CsvSource(SourceTap):
    """CsvSource iterates over csv file.

    >>> import tempfile, csv
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileSource.add_arguments(parser)
    >>> with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
    ...     fieldnames = ['id', 'field1', 'field2']
    ...     writer = csv.DictWriter(tmpfile, fieldnames=fieldnames)
    ...     writer.writeheader() #doctest:+SKIP
    ...     writer.writerow({'id': 0, 'field1': 'value1', 'field2': 'value2'}) #doctest:+SKIP
    ...     tmpfile.flush()
    ...     config = parser.parse_args("--infile {}".format(tmpfile.name).split())
    ...     csvSource = CsvSource(config)
    ...     [m.dct["id"] for m in csvSource.read()]
    ['0']
    """

    kind = "CSV"

    def __init__(self, config, logger=pipelineLogger):
        super().__init__(config, logger)
        if config.infile == "-":
            self.infile = sys.stdin
        else:
            self.infile = open(config.infile, "r")
        self.reader = csv.DictReader(self.infile, config.csv_dialect)
        self.logger.info("CSV Source: %s", config.infile)

    def __repr__(self):
        return 'CsvSource("{}")'.format(self.infile.name)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument("--infile", type=str, required=True, help="input csv file")
        parser.add_argument(
            "--csv-dialect",
            type=str,
            default="excel",
            choices=["excel", "excel-tab", "unix"],
            help="csv format: excel or excel-tab",
        )

    def read(self):
        for row in self.reader:
            yield self.messageClass(row, config=self.config)


class CsvDestination(DestinationTap):
    """CsvDestination writes items to a csv file.

    >>> import os, tempfile, csv
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileDestination.add_arguments(parser)
    >>> tmpdir = tempfile.mkdtemp()
    >>> outFilename = os.path.join(tmpdir, 'outfile.csv')
    >>> config = parser.parse_args(args=[])
    >>> CsvDestination(config)
    CsvDestination("out-topic.csv")
    >>> config = parser.parse_args("--outfile {}".format(outFilename).split())
    >>> CsvDestination(config)
    CsvDestination("...outfile.csv")
    """

    kind = "CSV"

    def __init__(self, config, logger=pipelineLogger):
        super().__init__(config, logger)
        if config.outfile is None:
            if config.out_topic.find(".") >= 0:
                self.filename = config.out_topic
            else:
                self.filename = config.out_topic + ".csv"
        else:
            self.filename = config.outfile

        if config.outfile == "-":
            self.outFile = sys.stdout
        else:
            if config.overwrite:
                self.outFile = open(self.filename, "w")
            else:
                self.outFile = open(self.filename, "a")
        self.writer = None
        self.logger.info("CsvDestination: %s", self.filename)

    def __repr__(self):
        return 'CsvDestination("{}")'.format(self.filename)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument("--outfile", type=str, help="output json file")
        parser.add_argument(
            "--overwrite",
            action="store_true",
            default=False,
            help="overwrite output file instead of append",
        )
        parser.add_argument(
            "--csv-dialect",
            type=str,
            default="excel",
            choices=["excel", "excel-tab", "unix"],
            help="csv format: excel or excel-tab",
        )

    def write(self, message):
        if self.writer is None:
            self.writer = csv.DictWriter(
                self.outFile,
                fieldnames=message.dct.keys(),
                dialect=self.config.csv_dialect,
            )
            self.writer.writeheader()
        self.writer.writerow(message.dct)
        self.outFile.flush()

    def close(self):
        if self.filename != "-":
            self.outFile.close()
            self.logger.info("CsvDestination closed")
