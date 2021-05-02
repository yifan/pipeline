from unittest import TestCase

import pytest

from pipeline import Message, DescribeMessage, PipelineMessageError


class TestMessage(TestCase):
    def test_default(self):
        assert Message(content={}) is not None

    def test_dict(self):
        m = Message(content={"key": "value"})
        assert m.content.get("key") == "value"

    def test_serialization(self):
        message = Message(content={"key": "message"})
        result = Message.deserialize(message.serialize())
        self.assertDictEqual(message.content, result.content)

    def test_compression(self):
        content = b'{"key": "message is a message"}'
        c = Message._decompress(Message._compress(content))
        assert content == c

    def test_serialization_compression(self):
        message = Message(content={"key": "message"})
        result = Message.deserialize(message.serialize(compress=True))
        self.assertDictEqual(message.content, result.content)

    def test_serialization_compression_unicode(self):
        message = Message(content={"key": u"message \u6D88\u606F"})
        result = Message.deserialize(message.serialize(compress=True))
        self.assertDictEqual(message.content, result.content)

    def test_serialization_compression_verify(self):
        content = {"key": "message is a message", "text": "a dog ate a man"}
        message = Message(content=content)
        compressed = message.serialize(compress=True)
        message = Message(content=content)
        original = message.serialize()
        assert len(compressed) < len(original)

    def test_key(self):
        message = Message(id="m", content={"value": "*" * 2048})
        assert message.id == "m"

    def test_parsing_exception(self):
        with pytest.raises(PipelineMessageError):
            Message.deserialize(b"HFHGKDJFHG")

    def test_describe(self):
        DescribeMessage()
