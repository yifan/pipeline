import tempfile
from unittest import TestCase

from pipeline import DestinationTap, SourceTap, Message, TapKind


BASE_DIR = "./"


class TestTaps(TestCase):
    def test_file(self):
        destinationAndSettings = DestinationTap.of(TapKind.FILE)
        FileDestination = destinationAndSettings.destinationClass
        FileDestinationSettings = destinationAndSettings.settings
        with tempfile.NamedTemporaryFile() as tmpfile:
            outFilename = tmpfile.name
            settings = FileDestinationSettings()
            settings.parse_args(f"--out-filename {outFilename}".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            sourceAndSettings = SourceTap.of(TapKind.FILE)
            FileSource = sourceAndSettings.sourceClass
            FileSourceSettings = sourceAndSettings.settings
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {outFilename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )

    def test_csv_file(self):
        destinationAndSettings = DestinationTap.of(TapKind.CSV)
        FileDestination = destinationAndSettings.destinationClass
        FileDestinationSettings = destinationAndSettings.settings
        with tempfile.NamedTemporaryFile() as tmpfile:
            outFilename = tmpfile.name
            settings = FileDestinationSettings()
            settings.parse_args(f"--out-filename {outFilename}".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            sourceAndSettings = SourceTap.of(TapKind.CSV)
            FileSource = sourceAndSettings.sourceClass
            FileSourceSettings = sourceAndSettings.settings
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {outFilename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        assert message_written == message_read

    def test_file_stdout(self):
        destinationAndSettings = DestinationTap.of(TapKind.FILE)
        FileDestination = destinationAndSettings.destinationClass
        FileDestinationSettings = destinationAndSettings.settings
        settings = FileDestinationSettings()
        settings.parse_args("--out-filename -".split())
        destination = FileDestination(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

    def test_file_gz(self):
        destinationAndSettings = DestinationTap.of(TapKind.FILE)
        FileDestination = destinationAndSettings.destinationClass
        FileDestinationSettings = destinationAndSettings.settings
        settings = FileDestinationSettings()
        with tempfile.NamedTemporaryFile(suffix=".gz") as tmpfile:
            outFilename = tmpfile.name
            settings.parse_args(f"--out-filename {outFilename} --overwrite".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            sourceAndSettings = SourceTap.of(TapKind.FILE)
            FileSource = sourceAndSettings.sourceClass
            FileSourceSettings = sourceAndSettings.settings
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {outFilename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )
