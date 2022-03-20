from unittest import TestCase
from testprocessor import TestProcessor


class TestTestProcessor(TestCase):
    def test_worker(self):
        worker = TestProcessor()

        msgs = [{"input_value": "msg1"}, {"input_value": "msg2"}]
        worker.parse_args(args=["--in-kind", "MEM", "--out-kind", "MEM"])
        worker.source.load_data(msgs)
        worker.start()
        # make sure we get two results
        assert len(worker.destination.results) == len(msgs)
        result1 = worker.destination.results[0]
        assert result1.content.get("output_value") == "msg1 flag is off"
