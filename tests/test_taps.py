import tempfile
from unittest import TestCase

from pipeline import DestinationTap, SourceTap, Message, TapKind


BASE_DIR = "./"


class TestTaps(TestCase):
    def test_repr(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.MEM)
        settings = destination_and_settings_classes.settings_class()
        destination = destination_and_settings_classes.destination_class(settings)
        assert str(destination).startswith("Destination")

        source_and_settings_classes = SourceTap.of(TapKind.MEM)
        settings = source_and_settings_classes.settings_class()
        source = source_and_settings_classes.source_class(settings)
        assert str(source).startswith("Source")

    def test_file(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        with tempfile.NamedTemporaryFile() as tmpfile:
            out_filename = tmpfile.name
            settings = destination_and_settings_classes.settings_class()
            settings.parse_args(f"--out-filename {out_filename}".split())
            destination = destination_and_settings_classes.destination_class(settings)
            message_written = Message(content={"key": "written\U0001f604"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.FILE)
            settings = source_and_settings_classes.settings_class()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = source_and_settings_classes.source_class(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )

    def test_file_content_only(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        with tempfile.NamedTemporaryFile() as tmpfile:
            out_filename = tmpfile.name
            settings = destination_and_settings_classes.settings_class()
            settings.parse_args(
                f"--out-filename {out_filename} --out-content-only".split()
            )
            assert settings.content_only is True
            destination = destination_and_settings_classes.destination_class(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.FILE)
            settings = source_and_settings_classes.settings_class()
            settings.parse_args(
                f"--in-filename {out_filename} --in-content-only".split()
            )
            assert settings.content_only is True
            source = source_and_settings_classes.source_class(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )

    def test_csv_file(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.CSV)
        with tempfile.NamedTemporaryFile() as tmpfile:
            out_filename = tmpfile.name
            settings = destination_and_settings_classes.settings_class()
            settings.parse_args(f"--out-filename {out_filename}".split())
            destination = destination_and_settings_classes.destination_class(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.CSV)
            settings = source_and_settings_classes.settings_class()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = source_and_settings_classes.source_class(settings)
            message_read = next(source.read())

        assert message_written == message_read

    def test_file_stdout(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-filename -".split())
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

    def test_file_gz(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        settings = destination_and_settings_classes.settings_class()
        with tempfile.NamedTemporaryFile(suffix=".gz") as tmpfile:
            out_filename = tmpfile.name
            settings.parse_args(f"--out-filename {out_filename} --overwrite".split())
            destination = destination_and_settings_classes.destination_class(settings)
            message_written = Message(content={"key": "written"})
            destination.write(message_written)
            destination.close()

            source_and_settings_classes = SourceTap.of(TapKind.FILE)
            settings = source_and_settings_classes.settings_class()
            settings.parse_args(f"--in-filename {out_filename}".split())
            source = source_and_settings_classes.source_class(settings)
            message_read = next(source.read())

        self.assertEqual(
            message_written.content.get("key"), message_read.content.get("key")
        )
