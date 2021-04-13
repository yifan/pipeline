import tempfile
from unittest import TestCase

from pipeline import DestinationTap, SourceTap, Message, TapKind


BASE_DIR = "./"


class TestTaps(TestCase):
    def test_file(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        FileDestination = destination_and_settings_classes.destinationClass
        FileDestinationSettings = destination_and_settings_classes.settingsClass
        with tempfile.NamedTemporaryFile() as tmpfile:
            out_filename = tmpfile.name
            settings = FileDestinationSettings()
            settings.parse_args(f"--out-filename {out_filename}".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.FILE)
            FileSource = source_and_settings_classes.sourceClass
            FileSourceSettings = source_and_settings_classes.settingsClass
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )

    def test_csv_file(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.CSV)
        FileDestination = destination_and_settings_classes.destinationClass
        FileDestinationSettings = destination_and_settings_classes.settingsClass
        with tempfile.NamedTemporaryFile() as tmpfile:
            out_filename = tmpfile.name
            settings = FileDestinationSettings()
            settings.parse_args(f"--out-filename {out_filename}".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.CSV)
            FileSource = source_and_settings_classes.sourceClass
            FileSourceSettings = source_and_settings_classes.settingsClass
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        assert message_written == message_read

    def test_file_stdout(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        FileDestination = destination_and_settings_classes.destinationClass
        FileDestinationSettings = destination_and_settings_classes.settingsClass
        settings = FileDestinationSettings()
        settings.parse_args("--out-filename -".split())
        destination = FileDestination(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

    def test_file_gz(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        FileDestination = destination_and_settings_classes.destinationClass
        FileDestinationSettings = destination_and_settings_classes.settingsClass
        settings = FileDestinationSettings()
        with tempfile.NamedTemporaryFile(suffix=".gz") as tmpfile:
            out_filename = tmpfile.name
            settings.parse_args(f"--out-filename {out_filename} --overwrite".split())
            destination = FileDestination(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.FILE)
            FileSource = source_and_settings_classes.sourceClass
            FileSourceSettings = source_and_settings_classes.settingsClass
            settings = FileSourceSettings()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = FileSource(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )
