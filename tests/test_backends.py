from unittest import TestCase, mock

from pipeline import Message, TapKind, SourceTap, DestinationTap


class TestBackends(TestCase):
    @mock.patch("pipeline.backends.elasticsearch.Elasticsearch")
    def test_elastic(self, mock_elastic):
        results = {"hits": {"hits": []}}

        def mock_update(index, id, body):
            results["hits"]["hits"].append(
                {
                    "_source": body["doc"],
                }
            )

        mock_elastic.return_value.update.side_effect = mock_update
        mock_elastic.return_value.search.side_effect = [results]

        destination_and_settings_classes = DestinationTap.of(TapKind.ELASTIC)
        settings = destination_and_settings_classes.settings_class()
        settings.parse_args("--out-namespace out --out-topic test".split())
        destination = destination_and_settings_classes.destination_class(settings)
        message_written = Message(content={"key": "written"})
        destination.write(message_written)
        destination.close()

        source_and_settings_classes = SourceTap.of(TapKind.ELASTIC)
        settings = source_and_settings_classes.settings_class()
        settings.parse_args(
            "--in-namespace in --in-topic test --in-keyname key".split()
        )
        source = source_and_settings_classes.source_class(settings)
        messages_read = list(source.read())
        self.assertEquals(len(messages_read), 1)
        for message_read in messages_read:
            self.assertEquals(message_read.id, "written")
