import logging
import sys
import traceback
from abc import ABC
from argparse import ArgumentParser
from copy import copy
from datetime import datetime
from enum import IntEnum, Enum
from typing import Optional, List, Dict, Iterator, Type, KeysView, Union, cast, Any
from logging import Logger

from pydantic import BaseModel, ByteSize, Field, ValidationError, parse_obj_as
from pydantic.schema import model_schema

from .version import version
from .exception import PipelineError, PipelineInputError, PipelineOutputError
from .message import Message, Command, Log, Kind, MessageBase
from .monitor import WorkerMonitor as Monitor
from .tap import DestinationTap, SourceTap
from .tap import TapKind, SourceSettings, DestinationSettings  # noqa: F401
from .helpers import Settings, Timer


pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.INFO)


class CommandActions(str, Enum):
    Reset = "RESET"
    Define = "DEFINE"


class Definition(BaseModel):
    """ """

    name: str
    version: str
    description: str
    source: SourceSettings
    destination: Optional[DestinationSettings]
    input_schema: Dict[str, Any] = dict()
    output_schema: Dict[str, Any] = dict()

    @classmethod
    def new(cls, **data):
        source = data.get("source")
        data["source"] = SourceSettings(
            **source.dict(include=SourceSettings.__fields__.keys())
        )

        destination = data.get("destination")
        if destination:
            data["destination"] = DestinationSettings(
                **destination.dict(include=DestinationSettings.__fields__.keys())
            )
        if "input_class" in data:
            input_class = data.get("input_class")
            input_schema = model_schema(input_class, ref_prefix="#/components/schemas/")
            del data["input_class"]
            data["input_schema"] = input_schema
        if "output_class" in data:
            output_class = data.get("output_class")
            output_schema = model_schema(
                output_class, ref_prefix="#/components/schemas/"
            )
            del data["output_class"]
            data["output_schema"] = output_schema
        return cls(**data)


class WorkerType(IntEnum):
    Normal = 0
    NoInput = 1
    NoOutput = 2


class WorkerSettings(Settings):
    name: str
    version: str
    description: str
    in_kind: TapKind = Field(None, title="input kind")
    out_kind: TapKind = Field(None, title="output kind")
    debug: bool = Field(False, title="print DEBUG log")
    monitoring: bool = Field(False, title="enable prometheus monitoring")


class Worker(ABC):
    """Internal base class for pulsar-worker, DO NOT use it in your program!"""

    def __init__(
        self,
        settings: WorkerSettings,
        worker_type: WorkerType = WorkerType.Normal,
        logger: Logger = pipelineLogger,
    ) -> None:
        self.name = settings.name
        self.version = settings.version
        self.description = settings.description
        self.worker_type = worker_type
        self.settings = settings
        self.logger = logger
        self.timer = Timer()

        self.monitor = Monitor(self)

        self.logger.info(
            f"Pipeline Worker {self.name} {self.version} (pipeline {version})"
        )

    def setup(self) -> None:
        """loading code goes here"""
        pass

    def shutdown(self) -> None:
        """clean up code goes here"""
        pass

    def has_input(self) -> bool:
        return self.worker_type != WorkerType.NoInput

    def has_output(self) -> bool:
        return self.worker_type != WorkerType.NoOutput

    def parse_args(self, args: List[str] = sys.argv[1:]) -> None:
        parser = ArgumentParser(add_help=False)
        parser.add_argument(
            "-V",
            action="version",
            version=f"{self.version} (pipeline {version})",
            help="show version",
        )
        parser.add_argument(
            "-h", "--help", action="store_true", default=False, help="display help"
        )

        options, unknown = self.settings.parse_args(args, parser)

        if self.has_input() and self.settings.in_kind:
            self.source_and_settings_classes = SourceTap.of(self.settings.in_kind)
            source_settings = self.source_and_settings_classes.settings_class()
            source_settings.parse_args(args, parser)
            if not options.help:
                self.source = self.source_and_settings_classes.source_class(
                    settings=source_settings, logger=self.logger
                )
                self.logger.info(f"Source: {self.source}")

        if self.settings.out_kind:
            self.destination_and_settings_classes = DestinationTap.of(
                self.settings.out_kind
            )
            destination_settings = (
                self.destination_and_settings_classes.settings_class()
            )
            destination_settings.parse_args(args, parser)
            if not options.help:
                self.destination = (
                    self.destination_and_settings_classes.destination_class(
                        settings=destination_settings, logger=self.logger
                    )
                )
                self.logger.info(f"Destination: {self.destination}")
        else:
            self.worker_type = WorkerType.NoOutput

        if options.help:
            print(parser.format_help())
            sys.exit(0)

        if self.has_input() and self.settings.in_kind is None:
            self.logger.critical("Please specify '--in-kind' or environment 'IN_KIND'!")
            raise PipelineError("Please specify '--in-kind' or environment 'IN_KIND'!")

        # report worker info to monitor
        self.monitor.record_worker_info()


class ProducerSettings(WorkerSettings):
    """Producer settings"""

    pass


class Producer(Worker):
    """Producer is a worker to generate new messages. For example, a webcrawler can
    be a producer. It reads no input, and produce outputs until it exits.

    Parameters:
        :param settings: settings
        :type settings: ProducerSettings
        :param output_class: output class
        :type output_class: Type[BaseModel]
        :param logger: logger
        :type logger: Logger

    Usage:

    .. code-block:: python

        >>> from pydantic import BaseModel
        >>>
        >>> class Output(BaseModel):
        ...     pass
        >>>
        >>> settings = ProducerSettings(name='', version='', description='', out_kind='MEM')
        >>> producer = Producer(settings, output_class=Output)
        >>> producer.parse_args()
        >>> #producer.start()
    """

    generator: Iterator[BaseModel]

    def __init__(
        self,
        settings: ProducerSettings,
        output_class: Type[BaseModel],
        logger: Logger = pipelineLogger,
    ):
        super().__init__(settings, worker_type=WorkerType.NoInput, logger=logger)
        self.output_class = output_class

    def generate(self) -> Iterator[BaseModel]:
        """a producer to generate dict."""
        yield self.output_class()

    def _step(self) -> BaseModel:
        return next(self.generator)

    def start(self) -> None:
        try:
            options = self.settings
        except AttributeError:
            self.logger.critical("Did you forget to run .parse_args before start?")
            return

        self.logger.setLevel(level=logging.INFO)
        if options.debug:
            self.logger.setLevel(level=logging.DEBUG)

        self.logger.info("settings: write topic %s", self.destination.topic)

        self.setup()

        if self.settings.monitoring:
            self.monitor.expose()

        self.monitor.record_start()

        try:
            self.generator = self.generate()

            i = 0
            while True:
                i += 1
                log = Log(
                    name=self.name,
                    version=self.version,
                    updated=set(),
                    received=datetime.now(),
                )
                self.timer.start()
                output = self._step()
                self.timer.log(self.logger)
                if hasattr(output, "id"):
                    msg = Message(id=output.id, content=output.dict())  # type: ignore
                else:
                    msg = Message(content=output.dict())
                self.logger.info("Generated %d-th message %s", i, msg)
                log.updated.update(output.dict().keys())
                msg.logs.append(log)
                self.logger.info("Writing message %s", msg)
                size = ByteSize(self.destination.write(msg))
                self.logger.info(f"Message size: {size.human_readable()}")
                self.monitor.record_write(self.destination.topic)
        except StopIteration:
            pass
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.shutdown()

        self.destination.close()

        self.monitor.record_finish()


class SplitterSettings(WorkerSettings):
    in_kind: TapKind = Field(None, title="input kind")
    out_kind: TapKind = Field(None, title="output kind")


class Splitter(Worker):
    """Splitter will write to a topic whose name is based on a function"""

    def __init__(
        self,
        settings: SplitterSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger=logger)
        # keep a dictionary for 'topic': 'destination', self.destination is only used to parse
        # command line arguments
        self.destinations: Dict[str, DestinationTap] = {}

    def get_topic(self, msg: Message) -> str:
        raise NotImplementedError("You need to implement .get_topic(self, msg)")

    def _run_streaming(self) -> None:
        for msg in self.source.read():
            self.logger.info("Received message '%s'", str(msg))
            self.monitor.record_read(self.source.topic)

            log = Log(
                name=self.name,
                version=self.version,
                updated=set(),
                received=datetime.now(),
            )

            if msg.kind == Kind.Message:
                topic = self.get_topic(cast(Message, msg))

                if topic not in self.destinations:
                    settings = copy(self.destination.settings)
                    settings.topic = topic
                    self.destinations[
                        topic
                    ] = self.destination_and_settings_classes.destination_class(
                        settings, logger=self.logger
                    )

                destination = self.destinations[topic]

                msg.logs.append(log)

                self.logger.info("Writing message %s to topic <%s>", str(msg), topic)
                msgSize = destination.write(msg)
                self.logger.info(f"Message size: {msgSize}")

                self.monitor.record_write(topic)
            self.source.acknowledge()

    def start(self) -> None:
        self.logger.setLevel(level=logging.INFO)
        if self.settings.debug:
            self.logger.setLevel(level=logging.DEBUG)

        # if options.rewind:
        #     # consumer.seek(pulsar.MessageId.earliest)
        #     self.logger.info("seeked to earliest message available as requested")

        self.setup()

        self.logger.info("start listening")
        # if batch_mode:
        #  self._run_batch()
        # else:

        if self.settings.monitoring:
            self.monitor.expose()

        self.monitor.record_start()
        try:
            self._run_streaming()
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.shutdown()

        self.source.close()
        self.destination.close()
        self.monitor.record_finish()


class ProcessorSettings(WorkerSettings):
    limit: int = Field(
        None, title="set a limit to number of messages to process before exiting"
    )


class Processor(Worker):
    """Processor is a worker which will process incoming messages and output
    new messages

    """

    settings: ProcessorSettings
    input_class: Type[BaseModel]
    output_class: Type[BaseModel]
    destination_class: Optional[Type[DestinationTap]]

    def __init__(
        self,
        settings: ProcessorSettings,
        input_class: Type[BaseModel],
        output_class: Type[BaseModel],
        logger: Logger = pipelineLogger,
    ) -> None:
        if output_class is None:
            super().__init__(settings, worker_type=WorkerType.NoOutput, logger=logger)
        else:
            super().__init__(settings, logger=logger)
        self.retryEnabled = False
        self.input_class = input_class
        self.output_class = output_class
        self.destination_class = None

    def process(self, message_content: BaseModel, message_id: str) -> BaseModel:
        """process function to be overridden by users, for streaming
        processing, this function needs to do in-place update on msg.dct
        and return an error or a list of errors (for batch processing).
        Message has been terminated though .terminates() will be skipped
        in output.

        A typical process definition will be:

        .. code-block:: python
            :linenos:

            newValue = msg.value
            return OutputModel(value=newValue)
        """
        raise NotImplementedError("You need to implement .process()")

    def _step(self, msg: MessageBase) -> None:
        """process one message"""

        self.logger.info(f"Receive message {msg}")
        log = Log(
            name=self.name,
            version=self.version,
            updated=set(),
            received=datetime.now(),
        )

        if msg.kind == Kind.Command:
            updated = self.process_command(cast(Command, msg))
        elif msg.kind == Kind.Message:
            updated = self.process_message(cast(Message, msg))
        else:
            raise PipelineError("Unrecognized message kind")

        if updated:
            log.updated.update(updated)

        log.processed = datetime.now()
        log.elapsed = self.timer.elapsed_time()
        msg.logs.append(log)

        if self.has_output():
            size = self.destination.write(msg)
            self.logger.info(f"Wrote message {msg}(size:{size})")
            self.monitor.record_write(self.destination.topic)

    def process_message(self, msg: Message) -> Union[KeysView[str], None]:
        try:
            if isinstance(self.input_class, dict):
                input_data = msg.content
            else:
                input_data = msg.as_model(self.input_class)
            self.logger.info(f"Prepared input {input_data}")
        except ValidationError as e:
            self.logger.exception(
                f"Input validation failed for message {msg}", exc_info=e
            )
            raise PipelineInputError(f"Input validation failed for message {msg}")

        try:
            setattr(self, "message", msg)
            output_data = self.process(input_data, msg.id)
            self.logger.info(f"Processed message {msg}")
        except Exception:
            raise
        finally:
            delattr(self, "message")

        updated = None
        if self.has_output():
            try:
                output_model = parse_obj_as(self.output_class, output_data)
                self.logger.info(f"Validated output {output_model}")
            except ValidationError as e:
                self.logger.exception(
                    f"Output validation failed for message {msg}", exc_info=e
                )
                raise PipelineOutputError(f"Output validation failed for message {msg}")

            if output_model:
                updated = msg.update_content(output_model)

        return updated

    def process_command(self, cmd: Command) -> Union[KeysView[str], None]:
        updated = None
        if cmd.action == CommandActions.Define:
            dct = {
                "name": self.name,
                "version": self.version,
                "description": self.description,
            }
            dct["source"] = self.source.settings
            if self.has_output():
                dct["destination"] = self.destination.settings
            if self.input_class:
                dct["input_class"] = self.input_class
            if self.output_class:
                dct["output_class"] = self.output_class

            definition = Definition.new(**dct)
            updated = cmd.update_content(definition)
        else:
            raise PipelineInputError("Unknown command")
        return updated

    def start(self) -> None:
        """start processing."""

        self.logger.setLevel(level=logging.INFO)
        if self.settings.debug:
            self.logger.setLevel(level=logging.DEBUG)

        self.limit = (
            self.settings.limit - 1 if self.settings.limit else self.settings.limit
        )

        # if options.rewind:
        #     # consumer.seek(pulsar.MessageId.earliest)
        #     self.logger.info("seeked to earliest message available as requested")

        self.setup()

        self.logger.info(f"start reading from source {self.source}")
        if self.has_output():
            self.logger.info(f"will write to destination {self.destination}")

        if self.settings.monitoring:
            self.monitor.expose()

        self.monitor.record_start()

        for i, msg in enumerate(self.source.read()):

            self.monitor.record_read(self.source.topic)
            self.logger.info("Received %d-th message '%s'", i, str(msg))

            self.timer.start()
            with self.monitor.process_timer.time():
                try:
                    self._step(msg)
                except PipelineInputError as e:
                    # If input error, upstream worker should act
                    self.logger.error(traceback.format_exc())
                    self.monitor.record_error(str(e))
                except PipelineOutputError as e:
                    # If output error, worker needs to exit without acknowldgement
                    self.logger.error(traceback.format_exc())
                    self.monitor.record_error(str(e))
                    raise
                except Exception as e:
                    self.logger.error(traceback.format_exc())
                    self.monitor.record_error(str(e))

            self.timer.log(self.logger)

            self.source.acknowledge()

            if i == self.limit:
                self.logger.info(f"Limit {self.limit} reached, exiting")
                break

        self.shutdown()
        self.source.close()
        if self.has_output():
            self.destination.close()
        self.monitor.record_finish()
