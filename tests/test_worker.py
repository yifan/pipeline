import tempfile
import re
from unittest import mock

from pydantic import BaseModel, Field
import pytest

from pipeline import (
    TapKind,
    Command,
    ProducerSettings,
    Producer,
    ProcessorSettings,
    Processor,
    SplitterSettings,
    Splitter,
    PipelineOutputError,
    CommandActions,
    Definition,
)


def make_output(filename):
    class Output(BaseModel):
        key: int
        language: str

    class MyProducer(Producer):
        def generate(self):
            for i, language in enumerate(["en", "it", "ar"]):
                yield Output(key=i, language=language)

    settings = ProducerSettings(name="producer", version="0.1.0", description="")
    producer = MyProducer(settings, output_class=Output)
    producer.parse_args(args=f"--out-kind FILE --out-filename {filename}".split())
    producer.start()


class TestWorkerCore:
    def test_mem_producer(self, monkeypatch):
        class Output(BaseModel):
            key: int

        class MyProducer(Producer):
            def generate(self):
                for i in range(3):
                    yield Output(key=i)

        settings = ProducerSettings(
            name="producer",
            version="0.1.0",
            description="",
            out_kind=TapKind.MEM,
            debug=True,
            monitoring=True,
        )
        producer = MyProducer(settings, output_class=Output)
        producer.parse_args(args=["--out-topic", "test", "--debug"])
        monkeypatch.setattr(producer, "monitor", mock.MagicMock())
        producer.start()
        assert len(producer.destination.results) == 3

    def test_mem_producer_id(self):
        class Output(BaseModel):
            id: str

        class MyProducer(Producer):
            def generate(self):
                for i in range(3):
                    yield Output(id=str(i))

        settings = ProducerSettings(
            name="producer", version="0.1.0", description="", out_kind=TapKind.MEM
        )
        producer = MyProducer(settings, output_class=Output)
        producer.parse_args(args=["--out-topic", "test"])
        producer.start()
        assert len(producer.destination.results) == 3
        for i, result in enumerate(producer.destination.results):
            assert result.id == str(i)

    def test_processor_invalid_output(self):
        class Output(BaseModel):
            language: str

        class Input(BaseModel):
            key: int

        outputs = [Output(language="en"), Input(key=1), Output(language="en")]

        class MyProcessor(Processor):
            def process(self, msg, id):
                return outputs.pop(0)

        settings = ProcessorSettings(
            name="processor",
            version="0.1.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        msgs = [{"key": 1}, {"key": 2}, {"key": 3}]
        processor.parse_args()
        processor.source.data = msgs
        assert processor.has_output() is True
        with pytest.raises(PipelineOutputError):
            processor.start()

    def test_processor_has_output(self):
        class Input(BaseModel):
            key: str

        class MyProcessor(Processor):
            def process(self, input, id):
                pass

        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            monitoring=True,
        )
        processor = MyProcessor(settings, input_class=Input, output_class=None)
        processor.parse_args(
            args="--in-kind MEM --out-kind MEM --out-topic test".split()
        )
        assert processor.has_output() is False

    def test_processor_invalid_input(self):
        class Output(BaseModel):
            language: str

        class Input(BaseModel):
            language: str

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output(language="en")

        settings = ProcessorSettings(
            name="processor",
            version="0.1.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        msgs = [{"language": "en"}, {"key": 1}, {"key": 2}, {"language": "en"}]
        processor.parse_args()
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 2

    def test_processor_dict(self):
        class Output(BaseModel):
            key: str
            newkey: str

        class MyProcessor(Processor):
            def process(self, input, id):
                return Output(key=input.get("key"), newkey="newval")

        msgs = [{"key": "1"}, {"key": "2"}, {"key": "3"}]
        settings = ProcessorSettings(name="processor", version="0.0.0", description="")
        processor = MyProcessor(settings, input_class=dict, output_class=Output)
        processor.parse_args(
            args="--in-kind MEM --out-kind MEM --out-topic test".split()
        )
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 3
        m = processor.destination.results[0]
        assert m.get("newkey") == "newval"

    def test_processor(self):
        class Input(BaseModel):
            key: str

        class Output(BaseModel):
            key: str
            newkey: str

        class MyProcessor(Processor):
            def process(self, input, id):
                return Output(key=input.key, newkey="newval")

        msgs = [{"key": "1"}, {"key": "2"}, {"key": "3"}]
        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            monitoring=True,
        )
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        processor.parse_args(
            args="--in-kind MEM --out-kind MEM --out-topic test".split()
        )
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 3
        m = processor.destination.results[0]
        assert m.get("newkey") == "newval"

    def test_file(self):
        class Input(BaseModel):
            key: int
            language: str

        class Output(BaseModel):
            key: int
            language: str

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output(key=msg.key, language=msg.language)

        with tempfile.NamedTemporaryFile() as tmpInFile:
            make_output(tmpInFile.name)

            with tempfile.NamedTemporaryFile() as tmpOutFile:
                settings = ProcessorSettings(
                    name="processor", version="0.0.0", description=""
                )
                processor = MyProcessor(
                    settings, input_class=Input, output_class=Output
                )
                processor.parse_args(
                    args=[
                        "--in-kind",
                        "FILE",
                        "--in-filename",
                        tmpInFile.name,
                        "--out-kind",
                        "FILE",
                        "--out-filename",
                        tmpOutFile.name,
                    ]
                )
                processor.start()
                with open(tmpOutFile.name, "r") as f:
                    assert len([i for i in f]) == 3

    def test_file_stdin_stdout(self):
        class Input(BaseModel):
            key: int
            language: str

        class Output(BaseModel):
            key: int
            language: str

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output(key=msg.key, language=msg.language)

        with tempfile.NamedTemporaryFile() as tmpInFile:
            make_output(tmpInFile.name)

            settings = ProcessorSettings(
                name="processor", version="0.0.0", description=""
            )
            processor = MyProcessor(settings, input_class=Input, output_class=Output)
            processor.parse_args(
                args=[
                    "--in-kind",
                    "FILE",
                    "--in-filename",
                    tmpInFile.name,
                    "--out-kind",
                    "FILE",
                    "--out-filename",
                    "-",
                ]
            )
            processor.start()

    def test_mem_processor(self):
        class Input(BaseModel):
            pass

        class Output(BaseModel):
            pass

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output()

        msgs = [{}, {}, {}]
        settings = ProcessorSettings(name="processor", version="0.0.0", description="")
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        processor.parse_args(
            args="--in-kind MEM --out-kind MEM --out-topic test".split()
        )
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 3

    def test_mem_processor_nooutput(self):
        class Input(BaseModel):
            pass

        class Output(BaseModel):
            pass

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                assert self.message is not None
                raise NotImplementedError

        msgs = [{}, {}, {}]
        settings = ProcessorSettings(name="processor", version="0.0.0", description="")
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        processor.parse_args(args="--in-kind MEM --out-kind MEM".split())
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 0
        assert not hasattr(processor, "message")

    def test_mem_processor_raise(self):
        class Input(BaseModel):
            pass

        class Output(BaseModel):
            pass

        settings = ProcessorSettings(name="processor", version="0.0.0", description="")
        processor = Processor(settings, input_class=Input, output_class=Output)
        processor.parse_args(args=["--in-kind", "MEM"])
        assert processor.settings.out_kind is None
        assert not hasattr(processor, "destination")

    def test_mem_processor_limit(self):
        class Input(BaseModel):
            pass

        class Output(BaseModel):
            pass

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output()

        msgs = [{}, {}, {}]
        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        processor.parse_args(args=["--limit", "1", "--out-topic", "test"])
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 1

    def test_splitter(self):
        msgs = [{"language": "en"}, {"language": "it"}]
        settings = SplitterSettings(
            name="splitter",
            version="0.0.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )

        class MySplitter(Splitter):
            def get_topic(self, msg):
                return f'test-{msg.get("language")}'

        splitter = MySplitter(settings)
        splitter.parse_args(args=["--out-topic", "test"])
        splitter.source.data = msgs
        splitter.start()
        assert len(splitter.destinations["test-en"].results) == 1
        assert splitter.destinations["test-en"].results[0].content["language"] == "en"
        assert len(splitter.destinations["test-it"].results) == 1
        assert splitter.destinations["test-it"].results[0].content["language"] == "it"

    def test_splitter_file(self):
        with tempfile.NamedTemporaryFile() as tmpInFile:
            make_output(tmpInFile.name)

            with tempfile.NamedTemporaryFile() as tmpOutFile:
                settings = SplitterSettings(
                    name="splitter", version="0.0.0", description=""
                )

                class MySplitter(Splitter):
                    def get_topic(self, msg):
                        return f'test-{msg.get("language")}'

                splitter = MySplitter(settings=settings)
                splitter.parse_args(
                    args=[
                        "--in-kind",
                        "FILE",
                        "--out-kind",
                        "FILE",
                        "--in-topic",
                        "test",
                        "--out-topic",
                        "test",
                        "--in-filename",
                        tmpInFile.name,
                        "--out-filename",
                        tmpOutFile.name,
                    ]
                )
                splitter.start()
                assert len(splitter.destinations) == 3

    def test_logging(self, monkeypatch):
        class Input(BaseModel):
            key: int

        class Output(BaseModel):
            key: int

        class MyProcessor(Processor):
            def process(self, msg, id):
                self.logger.info("logging")
                return msg

        logger = mock.MagicMock()
        msgs = [{"key": "1"}, {"key": "2"}, {"key": "3"}]
        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
            debug=True,
            monitoring=True,
        )
        processor = MyProcessor(
            settings, input_class=Input, output_class=Output, logger=logger
        )
        processor.parse_args(args="--out-topic test".split())
        processor.source.data = msgs
        monkeypatch.setattr(processor, "monitor", mock.MagicMock())
        processor.start()
        assert len(processor.destination.results) == 3
        logger.info.assert_any_call("logging")

    def test_define_command(self):
        class Input(BaseModel):
            key: int
            title: str

        class Output(BaseModel):
            key: int

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output(key=1)

        msgs = [Command(action=CommandActions.Define), {"key": "3", "title": "title"}]
        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )
        processor = MyProcessor(
            settings,
            input_class=Input,
            output_class=Output,
        )
        processor.parse_args(args="--out-topic test".split())
        processor.source.data = msgs
        processor.start()
        assert len(processor.destination.results) == 2
        result = processor.destination.results[0]
        assert result.get("input_schema")
        assert result.get("output_schema")

    def test_definition(self):
        class ComplexField(BaseModel):
            value: str

        class Input(BaseModel):
            text: str = Field(..., title="text", description="input text")
            value: ComplexField

        class Output(BaseModel):
            probability: float = Field(..., title="prob", description="probability")

        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
            in_kind=TapKind.MEM,
            out_kind=TapKind.MEM,
        )
        processor = Processor(
            settings,
            input_class=Input,
            output_class=Output,
        )
        processor.parse_args()

        definition1 = Definition.new(
            name="worker",
            version="0.1.0",
            description="worker",
            source=processor.source.settings,
            destination=processor.destination.settings,
            input_class=Input,
            output_class=Output,
        )

        definition2 = Definition.parse_raw(definition1.json())

        assert definition2.source.topic == "in-topic"

        assert definition1 == definition2

    def test_help(self, capfd):
        class Input(BaseModel):
            pass

        class Output(BaseModel):
            pass

        class MyProcessor(Processor):
            def process(self, msg: Input, id: str) -> Output:
                return Output()

        settings = ProcessorSettings(
            name="processor",
            version="0.0.0",
            description="",
        )
        processor = MyProcessor(settings, input_class=Input, output_class=Output)
        with pytest.raises(SystemExit):
            processor.parse_args(
                args=["--help", "--in-kind", "FILE", "--out-kind", "FILE"]
            )
        out, err = capfd.readouterr()
        assert re.search("--in-filename", out)
