import requests
import pytest  # NOQA
from unittest import TestCase, mock

import fakeredis

from pipeline import Message, TapKind, SourceTap, DestinationTap


class TestBackends(TestCase):
    def setUp(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.FILE)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-filename -".split())
        self.destination = destination_and_settings_classes.destination_class(settings)

    def tearDown(self):
        self.destination.close()

    @mock.patch("pipeline.backends.elasticsearch.Elasticsearch")
    def test_elastic(self, mock_elastic):
        results = {"hits": {"hits": []}}

        def mock_update(index, id, body):
            results["hits"]["hits"].append(
                {
                    "_id": id,
                    "_source": body["doc"],
                }
            )

        mock_elastic.return_value.update.side_effect = mock_update
        mock_elastic.return_value.search.side_effect = [results]

        destination_and_settings_classes = DestinationTap.of(TapKind.ELASTIC)
        settings = destination_and_settings_classes.settings_class(
            _args="--out-namespace out --out-topic test --out-uri uri".split()
        )
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(id="1234", content={"key": "written"})
        destination.write(message_written)
        destination.close()

        source_and_settings_classes = SourceTap.of(TapKind.ELASTIC)
        settings = source_and_settings_classes.settings_class(
            _args="--in-namespace in --in-topic test --in-uri uri".split()
        )
        source = source_and_settings_classes.source_class(settings)
        messages_read = list(source.read())
        self.assertEquals(len(messages_read), 1)
        for message_read in messages_read:
            self.assertEquals(message_read.id, "1234")
            self.destination.write(message_read)

    @mock.patch("pipeline.backends.mongodb.Collection")
    @mock.patch("pipeline.backends.mongodb.MongoClient")
    def test_mongodb(self, mock_mongo, mock_collection):
        queries = []
        results = []

        def mock_update(filt, doc, upsert):
            queries.append(filt)
            results.append(doc["$set"])

        mock_collection.return_value.update_one.side_effect = mock_update
        mock_collection.return_value.find.return_value = iter(results)

        destination_and_settings_classes = DestinationTap.of(TapKind.MONGO)
        settings = destination_and_settings_classes.settings_class(
            _args="--out-namespace out --out-topic test --out-database test --out-keyname key,secondary_key".split()
        )
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(content={"key": "written", "secondary_key": "some"})
        destination.write(message_written)
        destination.close()

        assert "key" in queries[0]
        assert "secondary_key" in queries[0]

        source_and_settings_classes = SourceTap.of(TapKind.MONGO)
        settings = source_and_settings_classes.settings_class(
            _args=(
                "--in-namespace in --in-topic test --in-database test "
                '--in-keyname key --query {"key":"written"}'
            ).split()
        )
        source = source_and_settings_classes.source_class(settings)
        messages_read = list(source.read())
        self.assertEquals(len(messages_read), 1)
        for message_read in messages_read:
            self.assertEquals(message_read.id, "written")
            self.destination.write(message_read)

    def test_redis(self):
        destination_and_settings_classes = DestinationTap.of(TapKind.XREDIS)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-namespace out".split())
        destination = destination_and_settings_classes.destination_class(settings)
        destination.redis = mock.MagicMock()
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

        source_and_settings_classes = SourceTap.of(TapKind.XREDIS)
        settings = source_and_settings_classes.settings_class(group="group")
        settings.parse_args("--in-namespace in".split())
        source = source_and_settings_classes.source_class(settings)
        source.redis = mock.MagicMock()
        # message_read = next(source.read())

    @mock.patch("pipeline.backends.rq.Redis.from_url")
    def test_rq(self, mock_redis):
        mock_redis.return_value = fakeredis.FakeStrictRedis()
        destination_and_settings_classes = DestinationTap.of(TapKind.RQ)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-topic test".split())
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

        source_and_settings_classes = SourceTap.of(TapKind.RQ)
        settings = source_and_settings_classes.settings_class()
        settings.parse_args("--in-topic test".split())
        source = source_and_settings_classes.source_class(settings)
        message_read = next(source.read())
        assert message_read.content["key"] == "written"

    @mock.patch("pipeline.backends.rq.Redis.from_url")
    def test_rq_zaman(self, mock_redis):
        mock_redis.return_value = fakeredis.FakeStrictRedis()
        destination_and_settings_classes = DestinationTap.of(TapKind.RQ)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-topic test --out-zaman".split())
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

        source_and_settings_classes = SourceTap.of(TapKind.RQ)
        settings = source_and_settings_classes.settings_class()
        settings.parse_args("--in-topic test --in-zaman".split())
        source = source_and_settings_classes.source_class(settings)
        message_read = next(source.read())
        assert message_read.content["key"] == "written"


def test_http_source(aiohttp_client, loop):
    source_and_settings_classes = SourceTap.of(TapKind.HTTP)
    settings = source_and_settings_classes.settings_class()
    settings.parse_args("--in-host 127.0.0.1 --in-port 8080".split())
    source = source_and_settings_classes.source_class(settings)

    client = loop.run_until_complete(aiohttp_client(source.app))

    loop.run_until_complete(
        client.post(
            "/",
            headers={"Content-Type": "application/octet-stream"},
            data=Message(id="0", content={"key": "written"}).serialize(),
        )
    )

    message_read = next(source.read())
    source.close()

    assert message_read.content["key"] == "written"


def test_http_destination(monkeypatch):
    message_written = []

    def mock_post(url, headers, data):
        message_written.append(Message.deserialize(data))
        return mock.MagicMock()

    monkeypatch.setattr(requests, "post", mock_post)
    destination_and_settings_classes = DestinationTap.of(TapKind.HTTP)
    settings = destination_and_settings_classes.settings_class()
    settings.parse_args("--out-host 127.0.0.1 --out-port 8080".split())
    destination = destination_and_settings_classes.destination_class(settings)
    destination.write(Message(content={"key": "written"}))
    destination.close()

    assert message_written[0].content["key"] == "written"
