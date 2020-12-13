import logging
from pipeline import ProcessorConfig, Processor
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("worker")


class TestProcessor(Processor):
    def __init__(self):
        config = ProcessorConfig()
        super().__init__(
            __worker__,
            __version__,
            "Test Processor",
            config,
        )

    def process(self, msg):
        msg.update(
            {
                "existingKey": 1,
                "newKey": True,
            }
        )
        return None


if __name__ == "__main__":
    worker = TestProcessor()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
