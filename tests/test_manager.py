import os
from unittest import TestCase, mock

from pipeline import TapKind, Pipeline, Message, PipelineError


class TestPipeline(TestCase):
    def test_pipeline(self):
        pipeline = Pipeline(in_kind=TapKind.MEM, out_kind=TapKind.MEM)
        pipeline.add_destination_topic("test")
        destination = pipeline.destination_of("test")
        destination.write(Message(content={"key": "dummy", "test": "value"}))
        self.assertEqual(len(destination.results), 1)

    def test_pipeline_environ(self):
        os.environ["IN_KIND"] = "MEM"
        os.environ["OUT_KIND"] = "MEM"
        pipeline = Pipeline()
        del os.environ["IN_KIND"]
        del os.environ["OUT_KIND"]
        pipeline.add_source_topic("test")
        assert pipeline.source_of("test") is not None
        pipeline.add_destination_topic("test")
        destination = pipeline.destination_of("test")
        destination.write(Message(content={"key": "dummy", "test": "value"}))
        self.assertEqual(len(destination.results), 1)

    def test_pipeline_topic_name(self):
        os.environ["IN_KIND"] = "LREDIS"
        os.environ["OUT_KIND"] = "LREDIS"
        os.environ["IN_NAMESPACE"] = "test"
        os.environ["OUT_NAMESPACE"] = "test"
        pipeline = Pipeline()
        del os.environ["IN_KIND"]
        del os.environ["OUT_KIND"]
        pipeline.add_source_topic("test")
        assert pipeline.source_of("test").topic == "test/test"
        pipeline.add_destination_topic("test")
        assert pipeline.destination_of("test").topic == "test/test"

    def test_pipeline_redis(self):
        os.environ["IN_KIND"] = "LREDIS"
        os.environ["OUT_KIND"] = "LREDIS"
        pipeline = Pipeline()
        del os.environ["IN_KIND"]
        del os.environ["OUT_KIND"]
        pipeline.add_source_topic("test")
        assert pipeline.source_of("test") is not None
        pipeline.add_destination_topic("test")
        destination = pipeline.destination_of("test")
        destination.redis = mock.MagicMock()
        destination.write(Message(content={"key": "dummy", "test": "value"}))

    def test_pipeline_unknown(self):
        os.environ["IN_KIND"] = "MEM"
        os.environ["OUT_KIND"] = "MEM"
        pipeline = Pipeline(args=["unknown"])
        del os.environ["IN_KIND"]
        del os.environ["OUT_KIND"]
        assert pipeline is not None

    def test_pipeline_notset(self):
        with self.assertRaises(PipelineError):
            Pipeline()
