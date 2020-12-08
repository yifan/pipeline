import argparse

from .tap import SourceOf, DestinationOf
from .helpers import parse_kind


class TapManager(object):
    """use TapManager to construct pipelines without worker logic

    >>> tapManager = TapManager('MEM')
    """

    def __init__(self, kind=None, noInput=False, noOutput=False):
        assert not (noInput and noOutput)

        parser = argparse.ArgumentParser(
            "pipeline", conflict_handler="resolve"
        )
        known, extras = parse_kind(["--kind", kind])

        if not noInput:
            self.sources = {}
            self.sourceClass = SourceOf(known.kind)
            self.sourceClass.add_arguments(parser)

        if not noOutput:
            self.destinations = {}
            self.destinationClass = DestinationOf(known.kind)
            self.destinationClass.add_arguments(parser)

        self.options = parser.parse_args(extras)

    def addSourceTopic(self, name):
        self.options.in_topic = name
        self.sources[name] = self.sourceClass(self.options)

    def addDestinationTopic(self, name):
        self.options.out_topic = name
        self.destinations[name] = self.destinationClass(self.options)

    def sourceOf(self, name):
        return self.sources[name]

    def destinationOf(self, name):
        return self.destinations[name]
