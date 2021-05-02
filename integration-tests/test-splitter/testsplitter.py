import logging
import random
from pipeline import SplitterSettings, Splitter
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger("worker")


class TestSplitter(Splitter):
    def __init__(self):
        settings = SplitterSettings(
            name=__worker__,
            version=__version__,
            description="Test Splitter",
        )
        super().__init__(settings)

    def get_topic(self, msg):
        return random.choice(["test-q-processor-a", "test-q-processor-b"])


if __name__ == "__main__":
    worker = TestSplitter()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
