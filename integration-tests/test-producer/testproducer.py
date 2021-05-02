import logging
from pydantic import BaseModel
from pipeline import ProducerSettings, Producer
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger("worker")


class Output(BaseModel):
    id: str
    value: int
    text: str


class TestProducer(Producer):
    def __init__(self):
        settings = ProducerSettings(
            name=__worker__,
            version=__version__,
            description="Test Producer",
        )
        super().__init__(settings, output_class=Output)

    def generate(self):
        for i in range(0, 1000):
            yield Output(
                id=f"message-{i}",
                value=i,
                text="text is not good" * 1024,
            )


if __name__ == "__main__":
    worker = TestProducer()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
