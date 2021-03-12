import argparse
import logging

from .helpers import parse_kind
from .tap import SourceOf, DestinationOf

pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.INFO)


class Pipeline(object):
    """Pipeline manages :class:`SourceTap` and :class:`DestinationTap` when you don't want to use
    predefined worker logic. Instead, you have access to :class:`SourceTap` and :class:`DestinationTap`
    directly.


    To use it:
    >>> Pipeline = Pipeline(kind='MEM')
    """

    def __init__(
        self, args=None, kind=None, noInput=False, noOutput=False, logger=pipelineLogger
    ):
        """Initialize Pipeline with kind and options to turn off input/output

        :param kind: underlining queuing system [MEM, FILE, KAFKA, PULSAR, LREDIS, RABBITMQ]
            MEM - Memory based, mostly for unittests
            FILE - File based queueing, for development
            KAFKA - Use Kafka
            PULSAR - Use Pulsar
            LREDIS - Use Redis List (no acknowledgement, no consumer group)
            RABBITMQ - Use RabbitMQ (no consumer group)
        :param noInput: do not create any :class:`SourceTap`
        :param noOutput: do not create any :class:`DestinationTap`
        """
        assert not (noInput and noOutput)

        parser = argparse.ArgumentParser("pipeline", conflict_handler="resolve")
        if kind is None:
            known, extras = parse_kind(args if args else [])
        else:
            known, extras = parse_kind(["--kind", kind])

        if not noInput:
            self.sources = {}
            self.sourceClass = SourceOf(known.kind)
            self.sourceClass.add_arguments(parser)

        if not noOutput:
            self.destinations = {}
            self.destinationClass = DestinationOf(known.kind)
            self.destinationClass.add_arguments(parser)

        self.options, _ = parser.parse_known_args(extras)
        self.logger = logger

    def addSourceTopic(self, name):
        """Add a new :class:`SourceTap` with a defined topic(queue) name

        :param name: a name given for the source topic
        """
        self.options.in_topic = name
        self.sources[name] = self.sourceClass(self.options, logger=self.logger)

    def addDestinationTopic(self, name):
        """Add a new :class:`DestinationTap` with a defined topic(queue) name

        :param name: a name given for the destination topic
        """
        self.options.out_topic = name
        self.destinations[name] = self.destinationClass(
            self.options, logger=self.logger
        )

    def sourceOf(self, name):
        """Return the :class:`SourceTap` of specified topic(queue) name"""
        return self.sources[name]

    def destinationOf(self, name):
        """Return the :class:`DestinationTap` of specified topic(queue) name"""
        return self.destinations[name]
