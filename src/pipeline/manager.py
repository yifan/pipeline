import logging
from copy import copy
from typing import Any, Dict

from .exception import PipelineError
from .tap import (
    TapKind,
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
    sourceClassAndSettings: SourceAndSettingsClasses
    destinations: Dict[str, DestinationTap]
    destinationClassAndSettings: DestinationAndSettingsClasses

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
            self.settings.in_kind = TapKind[in_kind]
        out_kind = kwargs.get("out_kind")
        if out_kind:
            self.settings.out_kind = TapKind[out_kind]
        self.settings.parse_args(args)

        if self.settings.in_kind:
            self.sources = {}
            self.sourceClassAndSettings = SourceTap.of(self.settings.in_kind)
            self.sourceSettings = self.sourceClassAndSettings.settingsClass()
            self.sourceSettings.parse_args(args)

        if self.settings.out_kind:
            self.destinations = {}
            self.destinationClassAndSettings = DestinationTap.of(self.settings.out_kind)
            self.destinationSettings = self.destinationClassAndSettings.settingsClass()
            self.destinationSettings.parse_args(args)

        if self.settings.in_kind is None and self.settings.out_kind is None:
            raise PipelineError("Either in_kind or out_kind needs to be set")

        self.logger = logger

    def addSourceTopic(self, name: str) -> None:
        """Add a new :class:`SourceTap` with a defined topic(queue) name

        :param name: a name given for the source topic
        """
        settings = copy(self.sourceSettings)
        settings.topic = name
        self.sources[name] = self.sourceClassAndSettings.sourceClass(
            settings=settings, logger=self.logger
        )

    def addDestinationTopic(self, name: str) -> None:
        """Add a new :class:`DestinationTap` with a defined topic(queue) name

        :param name: a name given for the destination topic
        """
        settings = copy(self.destinationSettings)
        settings.topic = name
        self.destinations[name] = self.destinationClassAndSettings.destinationClass(
            settings=settings, logger=self.logger
        )

    def sourceOf(self, name: str) -> SourceTap:
        """Return the :class:`SourceTap` of specified topic(queue) name"""
        return self.sources[name]

    def destinationOf(self, name: str) -> DestinationTap:
        """Return the :class:`DestinationTap` of specified topic(queue) name"""
        return self.destinations[name]
