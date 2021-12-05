import sys

from dotenv import load_dotenv
from pydantic import BaseModel, parse_obj_as, Field

from .worker import ProcessorSettings, Processor


load_dotenv(".env")


def copy():
    """copy message from source to destination

    use this to transfer file input to database, or from a database to another
    database.
    """

    class CopyProcessorSettings(ProcessorSettings):
        model_definition: str = Field("", title="optional model definition file")

    settings = CopyProcessorSettings(
        name="copy",
        version="0.1.0",
        description="Copy data from source to destination",
    )

    settings.parse_args(sys.argv)

    if settings.model_definition:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "model", settings.model_definition
        )
        foo = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = foo
        spec.loader.exec_module(foo)
        Model = foo.Model
    else:

        class Model(BaseModel):
            pass

    class CopyProcessor(Processor):
        def __init__(self):
            super().__init__(settings, input_class=Model, output_class=Model)

        def process(self, message_content, message_id):
            return parse_obj_as(Model, message_content)

    worker = CopyProcessor()
    worker.parse_args()
    worker.start()
