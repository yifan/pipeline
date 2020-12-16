from unittest import TestCase
from types import SimpleNamespace

from pipeline import Message


class TestMessage(TestCase):
    def test_default(self):
        m = Message({})
        assert m.updated is False

    def test_dict(self):
        m = Message({"key": "value"})
        assert m.get("key") == "value"

    def test_get_version(self):
        m = Message({})
        assert not m.get_version("tester")["version"]

    def test_versions(self):
        m = Message({})
        m.update_version("tester", [0, 1, 0])
        assert m.updated is True
        assert m.get_version("tester")["version"] == [0, 1, 0]
        assert m.get_version("tester")["order"] == 1
        m.updated = False
        m.update_version("tester", [0, 1, 1])
        assert m.updated is True
        assert m.get_version("tester")["version"] == [0, 1, 1]
        assert m.get_version("tester")["order"] == 1
        m.updated = False
        m.update_version("validator", [0, 0, 1])
        assert m.updated is True
        assert m.get_version("validator")["version"] == [0, 0, 1]
        assert m.get_version("validator")["order"] == 2

    def test_subclass(self):
        class NewMessage(Message):
            keyname = "mykey"
            pass

        newMessage1 = NewMessage({"key": "new1"})
        assert newMessage1.keyname == "mykey"

    def test_update_replace(self):
        m = Message({})
        m.update({"key": "value"})
        assert m.get("key") == "value"
        m.replace({"newkey": "value"})
        assert m.get("key") is None
        assert m.get("newkey") == "value"

    def test_serialization(self):
        message = Message({"key": "message"})
        result = Message.deserialize(message.serialize())
        self.assertDictEqual(message.header, result.header)
        self.assertDictEqual(message.dct, result.dct)

    def test_compression(self):
        content = {"key": "message is a message"}
        m = Message({})
        c = m._decompress(m._compress(content))
        assert content == c

    def test_serialization_compression(self):
        config = SimpleNamespace(compress=True)
        message = Message({"key": "message"}, config=config)
        self.assertTrue(message.config.compress)
        result = Message.deserialize(message.serialize())
        self.assertDictEqual(message.header, result.header)
        self.assertDictEqual(message.dct, result.dct)

    def test_serialization_compression_verify(self):
        content = {"key": "message is a message", "text": "a dog ate a man"}
        config = SimpleNamespace(compress=True)
        message = Message(content, config=config)
        compressed = message.serialize()
        message = Message(content)
        original = message.serialize()
        assert len(compressed) < len(original)

    def test_logging(self):
        message = Message({"key": "m", "value": "*" * 2048})
        assert len(message.log_content()) == 1024
