import tempfile
from unittest import TestCase
from argparse import ArgumentParser

from pipeline import DestinationTap, SourceTap, Message, DestinationOf, SourceOf

BASE_DIR = "./"


class DummySourceTap(SourceTap):
    kind = "DUMMY"

    def __next__(self):
        pass

    def rewind(self):
        pass


class DummyDestinationTap(DestinationTap):
    kind = "DUMMY"

    def send(self, msg):
        pass


class TestTaps(TestCase):
    def test_is_cls_of(self):
        self.assertTrue(DummySourceTap.is_cls_of("DUMMY"))
        self.assertTrue(DummyDestinationTap.is_cls_of("DUMMY"))

    def test_file(self):
        FileDestination = DestinationOf("FILE")
        parser = ArgumentParser()
        FileDestination.add_arguments(parser)
        with tempfile.NamedTemporaryFile() as tmpfile:
            outFilename = tmpfile.name
            config = parser.parse_args(
                "--outfile {} --overwrite".format(outFilename).split()
            )
            destination = FileDestination(config)
            message_written = Message({"key": "written"})
            destination.write(message_written)
            destination.close()

            FileSource = SourceOf("FILE")
            parser = ArgumentParser()
            FileSource.add_arguments(parser)
            config = parser.parse_args("--infile {}".format(outFilename).split())
            source = FileSource(config)
            message_read = next(source.read())

        self.assertEqual(message_written.get("key"), message_read.get("key"))
