import logging
from copy import copy
from typing import Any, Dict

from .exception import PipelineError
from .tap import (
    SourceTap,
    DestinationTap,
    SourceAndSettingsClasses,
    DestinationAndSettingsClasses,
)
from .worker import ProcessorSettings

pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.INFO)


class Pipeline(object):
    """Pipeline manages :class:`SourceTap` and :class:`DestinationTap` when you don't want to use
    predefined worker logic. Instead, you have access to :class:`SourceTap` and :class:`DestinationTap`
    directly.


    Usage:

    .. code-block:: python

      from .tap import TapKind, MemorySourceSettings, MemoryDestinationSettings
      in_settings = MemorySourceSettings()
      out_settings = MemoryDestinationSettings()
      pipeline = Pipeline( in_kind=TapKind.MEM, in_settings=in_settings, out_kind=TapKind.MEM, out_settings=out_settings)

    Take command line arguments:

    .. code-block:: python

      pipeline = Pipeline(args=sys.argv)

    Take environment settings:

    .. code-block:: python

      pipeline = Pipeline()
    """

    settings: ProcessorSettings
    sources: Dict[str, SourceTap]
    source_and_settings_classes: SourceAndSettingsClasses
    destinations: Dict[str, DestinationTap]
    destination_and_settings_classes: DestinationAndSettingsClasses

    def __init__(
        self,
        **kwargs: Any,
    ):
        """Initialize Pipeline with kind and options to turn off input/output

        :param in_kind: underlining queuing system [MEM, FILE, KAFKA, PULSAR, LREDIS, RABBITMQ]
        :param out_kind: underlining queuing system [MEM, FILE, KAFKA, PULSAR, LREDIS, RABBITMQ]

            MEM - Memory based, mostly for unittests
            FILE - File based queueing, for development
            KAFKA - Use Kafka
            PULSAR - Use Pulsar
            LREDIS - Use Redis List (no acknowledgement, no consumer group)
            RABBITMQ - Use RabbitMQ (no consumer group)
        """
        args = kwargs.get("args", None)
        logger = kwargs.get("logger", pipelineLogger)
        self.settings = ProcessorSettings(
            name="pipeline",
            version="",
            description="",
        )
        in_kind = kwargs.get("in_kind")
        if in_kind:
            self.settings.in_kind = in_kind
        out_kind = kwargs.get("out_kind")
        if out_kind:
            self.settings.out_kind = out_kind
        self.settings.parse_args(args)

        if self.settings.in_kind:
            self.sources = {}
            self.source_and_settings_classes = SourceTap.of(self.settings.in_kind)
            self.source_settings = self.source_and_settings_classes.settings_class()
            self.source_settings.parse_args(args)

        if self.settings.out_kind:
            self.destinations = {}
            self.destination_and_settings_classes = DestinationTap.of(
                self.settings.out_kind
            )
            self.destination_settings = (
                self.destination_and_settings_classes.settings_class()
            )
            self.destination_settings.parse_args(args)

        if self.settings.in_kind is None and self.settings.out_kind is None:
            raise PipelineError("Either in_kind or out_kind needs to be set")

        self.logger = logger

    def add_source_topic(self, name: str) -> None:
        """Add a new :class:`SourceTap` with a defined topic(queue) name

        :param name: a name given for the source topic
        """
        settings = copy(self.source_settings)
        settings.topic = name
        source = self.source_and_settings_classes.source_class(
            settings=settings, logger=self.logger
        )
        self.sources[name] = source
        self.logger.info(f"Source {source} added for {name}")

    def add_destination_topic(self, name: str) -> None:
        """Add a new :class:`DestinationTap` with a defined topic(queue) name

        :param name: a name given for the destination topic
        """
        settings = copy(self.destination_settings)
        settings.topic = name
        destination = self.destination_and_settings_classes.destination_class(
            settings=settings, logger=self.logger
        )
        self.destinations[name] = destination
        self.logger.info(f"Destination {destination} added for {name}")

    def source_of(self, name: str) -> SourceTap:
        """Return the :class:`SourceTap` of specified topic(queue) name"""
        return self.sources[name]

    def destination_of(self, name: str) -> DestinationTap:
        """Return the :class:`DestinationTap` of specified topic(queue) name"""
        return self.destinations[name]
