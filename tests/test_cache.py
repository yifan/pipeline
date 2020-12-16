from unittest import TestCase, mock
from types import SimpleNamespace

from pipeline import Message, CacheOf, CachedMessageClass


class CachedMessageTestCase(TestCase):
    def test_read(self):
        logger = mock.MagicMock()
        data = {"abcdefg": {"k1": "v2", "k2": "v2"}}
        cache = CacheOf("MEM")(
            SimpleNamespace(in_fields="k1,k2", out_fields="k1,k2", mem=data),
            logger=logger,
        )
        cls = CachedMessageClass(Message, cache)
        cached_message = cls({"key": "abcdefg", "k1": "v1"})
        self.assertEqual(cached_message.get("k1"), "v1")
        self.assertEqual(cached_message.get("k2"), "v2")

    def test_write(self):
        logger = mock.MagicMock()
        data = {}
        cache = CacheOf("MEM")(
            SimpleNamespace(in_fields="k1,k2", out_fields="k1,k2", mem=data),
            logger=logger,
        )
        cls = CachedMessageClass(Message, cache)
        cached_message = cls({"key": "abcdefg", "k1": "v1"})
        cached_message.update({"k1": "v2", "k2": "v2"})
        self.assertEqual(data["abcdefg"].get("k1"), "v2")
        self.assertEqual(data["abcdefg"].get("k2"), "v2")
