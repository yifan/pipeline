from pydantic import BaseModel

from .worker import ProcessorSettings, Processor


def copy():
    class Input(BaseModel):
        pass

    class Output(BaseModel):
        pass

    class CopyProcessor(Processor):
        def __init__(self):
            settings = ProcessorSettings(
                name="copy",
                version="0.1.0",
                description="Copy data from input to output",
            )
            super().__init__(settings, input_class=Input, output_class=Output)

        def process(self, message_content, message_id):
            return Output()

    worker = CopyProcessor()
    worker.parse_args()
    worker.start()
