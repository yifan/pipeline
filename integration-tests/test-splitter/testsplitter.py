import logging
import random
from pipeline import SplitterConfig, Splitter
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("worker")


class TestSplitter(Splitter):
    def __init__(self):
        config = SplitterConfig()
        super().__init__(
            __worker__,
            __version__,
            "test splitter",
            config,
        )

    def get_topic(self, msg):
        return random.choice(["test-q-processor-a", "test-q-processor-b"])


if __name__ == "__main__":
    worker = TestSplitter()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
