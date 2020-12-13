import logging
import time
from pipeline import GeneratorConfig, Generator
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("worker")


class TestGenerator(Generator):
    def __init__(self):
        config = GeneratorConfig()
        super().__init__(
            __worker__,
            __version__,
            "Test Generator",
            config,
        )

    def generate(self):
        for i in range(0, 1000):
            time.sleep(1)
            yield {
                "key": f"message-{i}",
                "value": i,
                "text": "text is not good" * 1024,
            }


if __name__ == "__main__":
    worker = TestGenerator()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
