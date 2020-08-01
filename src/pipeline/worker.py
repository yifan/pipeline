import argparse
import logging
import os
import sys
import traceback
from abc import ABC
from copy import copy

from .cache import CacheOf
from .exception import PipelineError
from .message import Message
from .monitor import Monitor
from .tap import DestinationOf, SourceOf
from .utils import parse_kind

logger = logging.getLogger('pipeline')
logger.setLevel(logging.INFO)


class WorkerConfig:
    NO_FLAG = 0
    NO_INPUT = 1
    NO_OUTPUT = 2

    def __init__(self, noInput=False, noOutput=False, messageClass=Message, cacheKind=None):
        if noInput and noOutput:
            raise ValueError('Worker cannot has no input AND no output')
        self.flag = WorkerConfig.NO_FLAG
        if noInput:
            self.flag = WorkerConfig.NO_INPUT
        if noOutput:
            self.flag = WorkerConfig.NO_OUTPUT
        self.messageClass = messageClass
        self.cacheKind = cacheKind

    def disable_input(self):
        self.flag = WorkerConfig.NO_INPUT

    def disable_output(self):
        self.flag = WorkerConfig.NO_OUTPUT


class WorkerCore(ABC):
    """Internal base class for pulsar-worker, DO NOT use it in your program!"""

    def __init__(self, name, version, description, config):
        self.name = name
        self.version = [int(x) for x in version.split('.')]
        if len(self.version) != 3:
            raise RuntimeError('Version format is not valid [x.x.x]')
        self.description = description
        self.kind = None
        self.logger = logger
        self.flag = config.flag
        self.messageClass = config.messageClass
        self.cacheKind = config.cacheKind

        self.parser = argparse.ArgumentParser(
            prog=name,
            description=description,
            conflict_handler='resolve',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )

        self.monitor = Monitor(self)

        self.logger.info("Pipeline Worker %s (%s)", name, version)

    def setup(self):
        """loading code goes here"""

    def has_no_input(self):
        return self.flag == WorkerConfig.NO_INPUT

    def has_no_output(self):
        return self.flag == WorkerConfig.NO_OUTPUT

    def parse_args(self, args=sys.argv[1:], config=None):
        known, extras = parse_kind(args)
        self.kind = known.kind
        if self.kind is None:
            logger.critical("Please specify pipeline kind with '--kind' or environment 'PIPELINE'!")
            return
        self.cacheKind = known.cacheKind or self.cacheKind
        if self.flag != WorkerConfig.NO_INPUT:
            self.sourceClass = SourceOf(self.kind)
        if self.flag != WorkerConfig.NO_OUTPUT:
            self.destinationClass = DestinationOf(self.kind)
        if self.cacheKind:
            self.cacheClass = CacheOf(self.cacheKind)
        self._add_arguments(self.parser)
        if config:
            self.parser.set_defaults(**config)
        self.parser.set_defaults(message=self.messageClass)
        self.options = self.parser.parse_args(extras)
        if self.flag != WorkerConfig.NO_INPUT:
            self.source = self.sourceClass(self.options, logger=self.logger)
        if self.flag != WorkerConfig.NO_OUTPUT:
            self.destination = self.destinationClass(self.options, logger=self.logger)
        if self.cacheKind:
            self.cache = self.cacheClass(self.options, logger=self.logger)
        # report worker info to monitor
        self.monitor.record_worker_info()

    def _add_arguments(self, parser):
        parser.add_argument('--debug', action='store_true',
                            default=os.environ.get('DEBUG', 'FALSE') == 'TRUE',
                            help='debug, more verbose logging')
        if self.flag != WorkerConfig.NO_INPUT:
            self.sourceClass.add_arguments(parser)
        if self.flag != WorkerConfig.NO_OUTPUT:
            self.destinationClass.add_arguments(parser)
        if self.cacheKind:
            self.cacheClass.add_arguments(parser)
        self.add_arguments(parser)

    def add_arguments(self, parser):
        """Add commandline arguments, to be override by child classes."""


class GeneratorConfig(WorkerConfig):
    pass


class Generator(WorkerCore):
    def __init__(self, name, version, description=None, config=GeneratorConfig()):
        config.disable_input()
        super().__init__(name, version, description, config)
        self.generator = None

    def generate(self):
        """a generator to generate dict."""
        yield self.messageClass()

    def step(self):
        if not self.generator:
            self.generator = self.generate()
        dct = next(self.generator)
        msg = self.messageClass(dct)
        msg.update_version(self.name, self.version)
        self.logger.info('Generated %s', msg)
        if msg.is_valid():
            self.logger.info('Writing %s', msg)
            self.destination.write(msg)
            self.monitor.record_write(self.destination.topic)
        else:
            self.logger.error('Generated message is invalid, skipping')
            self.logger.error(msg.log_info())
            self.logger.error(msg.log_content())
            raise PipelineError('Invalid message')

    def start(self, monitoring=False):
        try:
            options = self.options
        except AttributeError:
            logger.critical('Did you forget to run .parse_args before start?')
            return

        self.logger.setLevel(level=logging.INFO)
        if options.debug:
            self.logger.setLevel(level=logging.DEBUG)

        self.logger.info('settings: write topic %s', options.out_topic)

        self.setup()

        if monitoring:
            self.monitor.expose()

        self.monitor.record_start()

        try:
            while True:
                self.step()
        except StopIteration:
            pass
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))
        self.destination.close()

        self.monitor.record_finish()


class SplitterConfig(WorkerConfig):
    pass


class Splitter(WorkerCore):
    """ Splitter will write to a topic whose name is based on a function
    """
    def __init__(self, name, version, description=None, config=SplitterConfig()):
        super().__init__(name, version, description, config)
        # keep a dictionary for 'topic': 'destination', self.destination is only used to parse
        # command line arguments
        self.destinations = {}

    def get_topic(self, msg):
        return '{}-{}'.format(self.destination.topic, msg.get('language', ''))

    def _run_streaming(self):
        for msg in self.source.read():
            self.logger.info("Received message '%s'", str(msg))
            self.monitor.record_read(self.source.topic)

            topic = self.get_topic(msg)

            if topic not in self.destinations:
                config = copy(self.destination.config)
                config.out_topic = topic
                self.destinations[topic] = self.destinationClass(config, logger=self.logger)

            destination = self.destinations[topic]

            if msg.is_valid():
                self.logger.info("Writing message %s to topic <%s>", str(msg), topic)
                destination.write(msg)
            else:
                self.logger.error('Produced message is invalid, skipping')
                self.logger.error(msg.log_info())
                self.logger.error(msg.log_content())
            self.monitor.record_write(topic)
            self.source.acknowledge()

    def start(self, monitoring=False):
        try:
            options = self.options
        except AttributeError:
            self.logger.critical('Did you forget to run .parse_args before start?')
            raise

        self.logger.setLevel(level=logging.INFO)
        if options.debug:
            self.logger.setLevel(level=logging.DEBUG)

        if options.rewind:
            # consumer.seek(pulsar.MessageId.earliest)
            self.logger.info('seeked to earliest message available as requested')

        self.setup()

        self.logger.info('start listening')
        # if batch_mode:
        #  self._run_batch()
        # else:

        if monitoring:
            self.monitor.expose()

        self.monitor.record_start()
        try:
            self._run_streaming()
            self.source.close()
            self.destination.close()
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))
        self.monitor.record_finish()


class ProcessorConfig(WorkerConfig):
    def __init__(self, noInput=False, noOutput=False, messageClass=Message, cacheKind=None, limit=None):
        super().__init__(noInput, noOutput, messageClass, cacheKind)
        self.limit = limit


class Processor(WorkerCore):
    def __init__(self, name, version, description=None, config=ProcessorConfig()):
        super().__init__(name, version, description, config)
        self.retryEnabled = False
        self.limit = config.limit - 1 if config.limit else config.limit

    def use_retry_topic(self, name=None):
        """ Retry topic is introduced to solve error handling by saving
            message being processed when error occurs to a separate topic.
            In this way, these messages can be reprocessed at a later stage.
        """
        config = copy(self.source.config)
        config.out_topic = name if name else self.name + "-retry"
        self.retryDestination = self.destinationClass(config, logger=self.logger)
        self.retryEnabled = True

    def process(self, msg):
        """ process function to be overridden by users, for streaming
            processing, this function needs to do in-place update on msg.dct
            and return an error or a list of errors (for batch processing).
            Message has been terminated though .terminates() will be skipped
            in output.
            A typical process definition will be:
                value = msg.get('preExistingKey')
                updates = {
                    'newKey': 'newValue',
                }
                msg.update(updates)
        """
        return None

    def _step(self, msg):
        """ process one message """
        failedOnError = False

        # process message
        if msg.should_update(self.name, self.version):
            self.logger.info("Processing message '%s'", str(msg))

            try:
                err = self.process(msg)
            except Exception:
                self.logger.error(traceback.format_exc())
                self.logger.error(msg.log_content())
                raise

            if err:
                self.logger.error("Error has occurred for message '%s': %s", str(msg), err)
                failedOnError = True
            else:
                msg.update_version(self.name, self.version)
        else:
            self.logger.warning('Message has been processed by higher version processor, no processed')

        # skip validation and output for some cases
        if msg.terminated:
            self.logger.info('Message<%s> terminates here', str(msg))
        elif not any((failedOnError, self.has_no_output())):
            if msg.is_valid():
                self.logger.info("Writing message '%s'", str(msg))
                self.destination.write(msg)
                self.monitor.record_write(self.destination.topic)
            else:
                failedOnError = True
                self.logger.error('result message is invalid, skipping')
                self.logger.warning(msg.log_info())
                self.logger.warning(msg.log_content())

        # retry if necessary
        if failedOnError and self.retryEnabled:
            self.logger.warning('message is sent to retry topic %s', self.retryDestination.config.out_topic)
            self.retryDestination.write(msg)
            self.monitor.record_write(self.retryDestination.topic)

    def _run_streaming(self):
        """ streaming processing messages from source and write resulted messages to destination

            error handling:
            1) error indicated by .process returning non-None value
                message will be written to retry topic if retry topic is enabled.
                WARNING: The message may have been modified during process().
            2) the result message is invalid
                the result message will be written to retry topic if retry topic is enabled.
                The message is processed, and invalid.
                WARNING: Processor will not re-process message unless the version of processor
                is higher than the version stored in message info.
        """
        for i, msg in enumerate(self.source.read()):
            self.monitor.record_read(self.source.topic)
            self.logger.info("Received %d-th message '%s'", i, str(msg))

            self._step(msg)

            self.source.acknowledge()

            if i == self.limit:
                return

    def start(self, batch_mode=False, monitoring=False):
        """ start processing. """
        try:
            options = self.options
        except AttributeError:
            self.logger.critical('Did you forget to run .parse_args before start?')
            return

        self.logger.setLevel(level=logging.INFO)
        if options.debug:
            self.logger.setLevel(level=logging.DEBUG)

        if options.rewind:
            # consumer.seek(pulsar.MessageId.earliest)
            self.logger.info('seeked to earliest message available as requested')

        self.setup()

        self.logger.info('start listening on topic %s', self.source.topic)
        # if batch_mode:
        #  self._run_batch()
        # else:

        if monitoring:
            self.monitor.expose()

        self.monitor.record_start()

        try:
            self._run_streaming()
            self.source.close()
            if not self.has_no_output():
                self.destination.close()
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.monitor.record_finish()
