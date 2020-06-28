import argparse
import logging
import os
import sys
import traceback
from abc import ABC
from copy import copy

from .data import DataReaderOf, DataWriterOf
from .exception import PipelineError
from .message import Message
from .monitor import Monitor
from .tap import DestinationOf, SourceOf
from .utils import parse_kind

logger = logging.getLogger('pipeline')
logger.setLevel(logging.INFO)


class WorkerCore(ABC):
    """Internal base class for pulsar-worker, DO NOT use it in your program!"""
    NO_FLAG = 0
    NO_INPUT = 1
    NO_OUTPUT = 2

    def __init__(self, name, version, description, flag, messageClass, dataKind):
        self.name = name
        self.version = [int(x) for x in version.split('.')]
        if len(self.version) != 3:
            raise RuntimeError('Version format is not valid [x.x.x]')
        self.description = description
        self.kind = None
        self.logger = logger
        self.flag = flag
        self.messageClass = messageClass

        self.dataKind = dataKind

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
        return self.flag == self.NO_INPUT

    def has_no_output(self):
        return self.flag == self.NO_OUTPUT

    def parse_args(self, args=sys.argv[1:], config=None):
        known, extras = parse_kind(args)
        self.kind = known.kind
        self.dataKind = known.dataKind or self.dataKind
        if self.flag != self.NO_INPUT:
            self.sourceClass = SourceOf(self.kind)
            if self.dataKind:
                self.dataReaderClass = DataReaderOf(self.dataKind)
        if self.flag != self.NO_OUTPUT:
            self.destinationClass = DestinationOf(self.kind)
            if self.dataKind:
                self.dataWriterClass = DataWriterOf(self.dataKind)
        self._add_arguments(self.parser)
        if config:
            self.parser.set_defaults(**config)
        self.parser.set_defaults(message=self.messageClass)
        self.options = self.parser.parse_args(extras)
        if self.flag != self.NO_INPUT:
            self.source = self.sourceClass(self.options, logger=self.logger)
            if self.dataKind:
                self.dataReader = self.dataReaderClass(self.options, logger=self.logger)
        if self.flag != self.NO_OUTPUT:
            self.destination = self.destinationClass(self.options, logger=self.logger)
            if self.dataKind:
                self.dataWriter = self.dataWriterClass(self.options, logger=self.logger)
        # report worker info to monitor
        self.monitor.record_worker_info()

    def _add_arguments(self, parser):
        parser.add_argument('--debug', action='store_true', default=os.environ.get('DEBUG', 'FALSE') == 'TRUE',
                            help='debug, more verbose logging')
        if self.flag != self.NO_INPUT:
            self.sourceClass.add_arguments(parser)
            if self.dataKind:
                self.dataReaderClass.add_arguments(parser)
        if self.flag != self.NO_OUTPUT:
            self.destinationClass.add_arguments(parser)
            if self.dataKind:
                self.dataWriterClass.add_arguments(parser)
        self.add_arguments(parser)

    def add_arguments(self, parser):
        """Add commandline arguments, to be override by child classes."""


class Generator(WorkerCore):
    def __init__(self, name, version, description=None, messageClass=Message, dataKind=None):
        super().__init__(name, version, description, Generator.NO_INPUT, messageClass, dataKind)

    def generate(self):
        """a generator to generate dict."""
        yield self.messageClass()

    def _generate(self):
        for i in self.generate():
            yield self.messageClass(i)

    def start(self, monitoring=False):
        try:
            options = self.options
        except AttributeError as e:
            logger.critical('Did you forget to run .parse_args before start?')
            raise e

        self.logger.setLevel(level=logging.INFO)
        if options.debug:
            self.logger.setLevel(level=logging.DEBUG)

        self.logger.info('settings: write topic %s', options.out_topic)

        self.setup()

        if monitoring:
            self.monitor.expose()

        self.monitor.record_start()

        try:
            for msg in self._generate():
                msg.update_version(self.name, self.version)
                self.logger.info('Generated %s', msg)
                if msg.is_valid():
                    self.destination.write(msg)
                else:
                    self.logger.error('Generated message is invalid, skipping')
                    self.logger.error(msg.log_info())
                    self.logger.error(msg.log_content())
                self.monitor.record_write(self.destination.topic)
            self.destination.close()
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.monitor.record_finish()


class Splitter(WorkerCore):
    """ Splitter will write to a topic whose name is based on a function
    """
    def __init__(self, name, version, description=None, messageClass=Message, dataKind=None):
        super().__init__(name, version, description, Splitter.NO_FLAG, messageClass, dataKind)
        # keep a dictionary for 'topic': 'destination', self.destination is only used to parse
        # command line arguments
        self.destinations = {}

    def get_topic(self, dct):
        return '{}-{}'.format(self.destination.topic, dct['language'])

    def _run_streaming(self):
        for msg in self.source.read():
            self.logger.info("Received message '%s'", str(msg))
            self.monitor.record_read(self.source.topic)

            topic = self.get_topic(msg.dct)

            if topic not in self.destinations:
                config = copy(self.destination.config)
                config.out_topic = topic
                self.destinations[topic] = self.destinationClass(config, logger=self.logger)

            destination = self.destinations[topic]

            if msg.is_valid():
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
        except AttributeError as e:
            self.logger.critical('Did you forget to run .parse_args before start?')
            raise e

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


class Processor(WorkerCore):
    def __init__(self, name, version, description=None, nooutput=False, messageClass=Message, dataKind=None):
        flag = Processor.NO_OUTPUT if nooutput else None
        super().__init__(name, version, description, flag, messageClass, dataKind)
        self.retryEnabled = False

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

    def _process(self, msg):
        try:
            return self.process(msg)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.logger.error(msg.log_content())
            raise e

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
        for msg in self.source.read():
            self.monitor.record_read(self.source.topic)
            self.logger.info("Received message '%s'", str(msg))

            failedOnError = False

            # process message
            if msg.should_update(self.name, self.version):
                self.logger.info("Processing message '%s'", str(msg))
                err = self._process(msg)
                if err:
                    self.logger.error("Error has occurred for message '%s': %s", str(msg), err)
                    failedOnError = True
                else:
                    msg.update_version(self.name, self.version)
            else:
                self.logger.warn('Message has been processed by higher version processor, no processed')

            # skip validation and output for some cases
            if msg.terminated:
                self.logger.info('Message<%s> terminates here', str(msg))
            elif not any((failedOnError, self.flag == Processor.NO_OUTPUT)):
                if msg.is_valid():
                    self.logger.info("Writing message '%s'", str(msg))
                    self.destination.write(msg)
                    self.monitor.record_write(self.destination.topic)
                else:
                    failedOnError = True
                    logger.error('result message is invalid, skipping')
                    logger.warn(msg.log_info())
                    logger.warn(msg.log_content())

            # retry if necessary
            if failedOnError and self.retryEnabled:
                self.logger.warn('message is sent to retry topic %s', self.retryDestination.config.out_topic)
                self.retryDestination.write(msg)
                self.monitor.record_write(self.retryDestination.topic)

            self.source.acknowledge()

    def start(self, batch_mode=False, monitoring=False):
        """ start processing. """
        try:
            options = self.options
        except AttributeError as e:
            self.logger.critical('Did you forget to run .parse_args before start?')
            raise e

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
            if self.flag != self.NO_OUTPUT:
                self.destination.close()
        except PipelineError as e:
            e.log(self.logger)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            self.monitor.record_error(str(e))

        self.monitor.record_finish()
