
from unittest import TestCase

from pipeline import Message


class TestMessage(TestCase):
    def test_default(self):
        m = Message()
        assert m.updated is False

    def test_get_version(self):
        m = Message()
        assert not m.get_version('tester')['version']

    def test_versions(self):
        m = Message()
        m.update_version('tester', [0, 1, 0])
        assert m.updated is True
        assert m.get_version('tester')['version'] == [0, 1, 0]
        assert m.get_version('tester')['order'] == 1
        m.updated = False
        m.update_version('tester', [0, 1, 1])
        assert m.updated is True
        assert m.get_version('tester')['version'] == [0, 1, 1]
        assert m.get_version('tester')['order'] == 1
        m.updated = False
        m.update_version('validator', [0, 0, 1])
        assert m.updated is True
        assert m.get_version('validator')['version'] == [0, 0, 1]
        assert m.get_version('validator')['order'] == 2

    def test_subclass(self):
        class NewMessage(Message):
            def __init__(self, other=None):
                super().__init__()
                if isinstance(other, dict):
                    self.dct = other
                else:
                    self.info, self.dct = other.info, other.dct

        newMessage1 = NewMessage({'key': 'new1'})
        newMessage2 = NewMessage(newMessage1)
        assert newMessage2.dct['key'] == 'new1'

    def test_update_replace(self):
        m = Message()
        m.update({'key': 'value'})
        assert m.get('key') == 'value'
        m.replace({'newkey': 'value'})
        assert m.get('key') is None
        assert m.get('newkey') == 'value'
