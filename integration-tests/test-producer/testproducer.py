import logging
from pydantic import BaseModel
from pipeline import ProducerSettings, Producer
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger("worker")


class TestProducerSettings(ProducerSettings):
    limit: int = 10


class Output(BaseModel):
    id: str
    value: int
    text: str


class TestProducer(Producer):
    def __init__(self):
        settings = TestProducerSettings(
            name=__worker__,
            version=__version__,
            description="Test Producer",
        )
        super().__init__(settings, output_class=Output)

    def generate(self):
        for i in range(self.settings.limit):
            yield Output(
                id=f"message-{i}",
                value=i,
                text="text is not good",
            )


if __name__ == "__main__":
    worker = TestProducer()
    worker.parse_args()
    worker.start()
