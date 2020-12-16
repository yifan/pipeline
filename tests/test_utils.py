from unittest import TestCase

from pipeline import Pipeline, Message


class TestPipeline(TestCase):
    def test_pipeline(self):
        pipeline = Pipeline("MEM")
        pipeline.addDestinationTopic("test")
        destination = pipeline.destinationOf("test")
        destination.write(Message({"key": "dummy", "test": "value"}))
        self.assertEqual(len(destination.results), 1)
