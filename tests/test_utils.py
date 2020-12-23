import os

from unittest import TestCase, mock

from pipeline import Pipeline, Message, PipelineError


class TestPipeline(TestCase):
    def test_pipeline(self):
        pipeline = Pipeline(kind="MEM")
        pipeline.addDestinationTopic("test")
        destination = pipeline.destinationOf("test")
        destination.write(Message({"key": "dummy", "test": "value"}))
        self.assertEqual(len(destination.results), 1)

    def test_pipeline_environ(self):
        os.environ["PIPELINE"] = "MEM"
        pipeline = Pipeline()
        del os.environ["PIPELINE"]
        pipeline.addDestinationTopic("test")
        destination = pipeline.destinationOf("test")
        destination.write(Message({"key": "dummy", "test": "value"}))
        self.assertEqual(len(destination.results), 1)

    @mock.patch("redis.Redis")
    def test_pipeline_redis(self, r):
        os.environ["PIPELINE"] = "LREDIS"
        pipeline = Pipeline()
        del os.environ["PIPELINE"]
        pipeline.addSourceTopic("test")
        assert pipeline.sourceOf("test") is not None
        pipeline.addDestinationTopic("test")
        destination = pipeline.destinationOf("test")
        destination.write(Message({"key": "dummy", "test": "value"}))

    def test_pipeline_notset(self):
        with self.assertRaises(PipelineError):
            Pipeline()
