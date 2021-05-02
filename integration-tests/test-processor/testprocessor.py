import logging
from pydantic import BaseModel
from pipeline import ProcessorSettings, Processor
from version import __worker__, __version__


FORMAT = "%(asctime)-15s %(levelno)s %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger("worker")


class Input(BaseModel):
    id: str
    value: int
    text: str


class Output(BaseModel):
    id: str
    flag: bool
    additional: int


class TestProcessor(Processor):
    def __init__(self):
        settings = ProcessorSettings(
            name=__worker__, version=__version__, description="Test Processor"
        )
        super().__init__(
            settings,
            input_class=Input,
            output_class=Output,
        )
        self.counter = 0

    def process(self, msg):
        self.counter += 1
        o = Output(
            id=msg.id,
            flag=True,
            additional=self.counter,
        )
        self.logger.info(o)
        return o


if __name__ == "__main__":
    worker = TestProcessor()
    worker.parse_args()
    if worker.options.debug:
        logger.setLevel(logging.DEBUG)
    worker.start()
