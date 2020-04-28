from unittest import TestCase

from pipeline import DestinationTap, SourceTap

BASE_DIR = './'


class DummySourceTap(SourceTap):
    kind = 'DUMMY'

    def __next__(self):
        pass

    def rewind(self):
        pass


class DummyDestinationTap(DestinationTap):
    kind = 'DUMMY'

    def send(self, msg):
        pass


class TestTaps(TestCase):
    def test_is_cls_of(self):
        self.assertTrue(DummySourceTap.is_cls_of('DUMMY'))
        self.assertTrue(DummyDestinationTap.is_cls_of('DUMMY'))
