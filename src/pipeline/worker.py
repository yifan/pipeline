import logging
import sys
import traceback
from abc import ABC
from copy import copy
from datetime import datetime
from enum import IntEnum
from typing import Optional, Set, List, Dict, Iterator, Type
from logging import Logger

from pydantic import BaseModel, ByteSize, Field, ValidationError

from .exception import PipelineError
from .message import Message
from .monitor import Monitor
from .tap import DestinationTap, SourceTap
from .tap import TapKind, SourceSettings, DestinationSettings  # noqa: F401
from .helpers import Settings, Timer


pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.INFO)


class Log(BaseModel):
    name: str
    version: str
    updated: Set[str]
    received: datetime = datetime.now()
    processed: Optional[datetime] = None
    elapsed: Optional[float] = None


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

        self.logger.info("Pipeline Worker %s (%s)", self.name, self.version)

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
        self.settings.parse_args(args)

        if self.has_input():
            if self.settings.in_kind is None:
                self.logger.critical(
                    "Please specify '--in-kind' or environment 'IN_KIND'!"
                )
                raise PipelineError(
                    "Please specify '--in-kind' or environment 'IN_KIND'!"
                )

            self.source_and_settings_classes = SourceTap.of(self.settings.in_kind)
            source_settings = self.source_and_settings_classes.settings_class()
            source_settings.parse_args(args)
            self.source = self.source_and_settings_classes.source_class(
                settings=source_settings, logger=self.logger
            )

        if self.settings.out_kind:
            self.destination_and_settings_classes = DestinationTap.of(
                self.settings.out_kind
            )
            destination_settings = (
                self.destination_and_settings_classes.settings_class()
            )
            destination_settings.parse_args(args)
            self.destination = self.destination_and_settings_classes.destination_class(
                settings=destination_settings, logger=self.logger
            )
        else:
            self.worker_type = WorkerType.NoOutput

        # report worker info to monitor
        self.monitor.record_worker_info()


class ProducerSettings(WorkerSettings):
    pass


class Producer(Worker):
    """Producer is a worker to generate new messages. For example, a webcrawler can
    be a producer. It reads no input, and produce outputs until it exits.

    Usage:
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

    generator: Iterator[Message]

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
                    msg = Message(id=output.id, content=output.dict())
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

            topic = self.get_topic(msg)

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

            # FIXME validation
            if True or msg.is_valid():
                self.logger.info("Writing message %s to topic <%s>", str(msg), topic)
                msgSize = destination.write(msg)
                self.logger.info(f"Message size: {msgSize}")
            else:
                self.logger.error("Produced message is invalid, skipping")
                self.logger.debug(msg.log_info())
                self.logger.debug(msg.log_content())
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
    settings: ProcessorSettings
    input_class: Type[BaseModel]
    output_class: Optional[Type[BaseModel]]
    destination_class: Optional[Type[DestinationTap]]

    def __init__(
        self,
        settings: ProcessorSettings,
        input_class: Type[BaseModel],
        output_class: Optional[Type[BaseModel]] = None,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger=logger)
        self.retryEnabled = False
        self.input_class = input_class
        self.output_class = output_class
        self.destination_class = None

    def use_retry_topic(self, name: str = None) -> None:
        """Retry topic is introduced to solve error handling by saving
        message being processed when error occurs to a separate topic.
        In this way, these messages can be reprocessed at a later stage.
        """
        if self.destination_class:
            settings = copy(self.destination.settings)
            settings.topic = name if name else self.name + "-retry"
            self.retryDestination = self.destination_class(
                settings=settings, logger=self.logger
            )
            self.retryEnabled = True
        else:
            raise PipelineError("Please call this function after parse_args")

    def process(self, msg: BaseModel) -> BaseModel:
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

    def _step(self, msg: Message) -> None:
        """ process one message """
        try:
            # each worker to append a log
            log = Log(
                name=self.name,
                version=self.version,
                updated=set(),
                received=datetime.now(),
            )
            self.logger.info(f"Receive message {msg}")
            input_data = msg.as_model(self.input_class)
            self.logger.info(f"Prepared input {input}")
            output_data = self.process(input_data)
            # force validate output
            output_model = self.output_class(**output_data.dict())
            self.logger.info(f"Processed message {msg}")
            if output_model:
                updated = msg.update_content(output_model)
                log.updated.update(updated)
            log.processed = datetime.now()
            log.elapsed = 0.0
            msg.logs.append(log)
        except ValidationError as e:
            # When input error, we don't expect to rerun with same input,
            # it is okay to print error and skip to next input
            self.logger.error(f"Input validation failed for message {msg}")
            self.logger.error(e.json())
            return
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(e)
            # self.logger.error(
            #     "message is sent to retry topic %s",
            #     self.retryDestination.config.out_topic,
            # )
            # self.retryDestination.write(msg)
            # self.monitor.record_write(self.retryDestination.topic)
            return

        if self.has_output():
            size = self.destination.write(msg)
            self.logger.info(f"Wrote message {msg}(size:{size})")
            self.monitor.record_write(self.destination.topic)

    def _run_streaming(self) -> None:
        """streaming processing messages from source and write resulted messages to destination


        1. error indicated by .process returning non-None value

            message will be written to retry topic if retry topic is enabled.
            WARNING: The message may have been modified during process().

        2. the result message is invalid

            the result message will be written to retry topic if retry topic is enabled.
            The message is processed, and invalid.
            WARNING: Processor will not re-process message unless the version of processor
            is higher than the version stored in message info.
        """
        for i, msg in enumerate(self.source.read()):
            self.monitor.record_read(self.source.topic)
            self.logger.info("Received %d-th message '%s'", i, str(msg))

            self.timer.start()
            with self.monitor.process_timer.time():
                self._step(msg)
            self.timer.log(self.logger)

            self.source.acknowledge()

            self.logger.info(f"{i} == {self.limit}")
            if i == self.limit:
                self.logger.info(f"Limit {self.limit} reached, exiting")
                break

    def start(self) -> None:
        """ start processing. """

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

        self.logger.info("start listening on topic %s", self.source.topic)
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
            self.logger.error(traceback.format_exc())
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.shutdown()
        self.source.close()
        if self.has_output():
            self.destination.close()
        self.monitor.record_finish()
