import json
from abc import ABC
import time
import traceback

from .exception import PipelineError


class Message(ABC):
    """Message is a container for data flowing in pipeline. To construct a message,
    you need to pass a dict in Message constructor. Message is expected to have a
    key (the name is defined in Message.keyname) in this dict

    Usage:
    >>> message = Message({'key': 'unique-id-1', 'data': 'data'})
    """

    keyname = "key"

    def __init__(self, other=None):
        self.updated = False
        self.terminated = False
        self.header = {}
        self.dct = {}
        try:
            if other is not None:
                if isinstance(other, type(self)):
                    self.header = other.header
                    self.dct = other.dct
                elif isinstance(other, dict):
                    self.dct = other
                elif isinstance(other, bytes):
                    [self.header, self.dct] = self.deserialize(other)
                elif isinstance(other, str):
                    [self.header, self.dct] = self.deserialize(other)
                else:
                    raise PipelineError(
                        'Message needs to be initialized with a message, a dict or str/bytes, not "{}"'.format(
                            type(other)
                        ),
                        data=other,
                    )
        except PipelineError as error:
            raise error
        except Exception as error:
            raise PipelineError(
                str(error), data=other, traceback=traceback.print_exception()
            )

    def __str__(self):
        return "{}<{}:{}>".format(
            type(self).__name__, self.keyname, self.dct.get(self.keyname, None)
        )

    def __repr__(self):
        return self.__str__()

    def __unicode__(self):
        return self.__str__()

    def log(self, logger):
        logger.info(self.log_header())
        logger.info(self.log_content())

    def log_info(self):
        """ for compatibility only """
        return self.log_header()

    def log_header(self):
        return json.dumps(self.header, indent=4)

    def log_content(self):
        return json.dumps(self.dct, indent=4)

    @classmethod
    def add_arguments(cls, parser):
        return parser

    def serialize(self, indent=None):
        """serialize to binary."""
        return json.dumps([self.header, self.dct], indent=indent).encode(
            "utf-8"
        )

    @classmethod
    def deserialize(cls, raw):
        """deserialize to json."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)

    def get_version(self, name):
        return self.header.setdefault(
            name,
            {
                "version": [],
            },
        )

    def get_versions(self):
        return self.header

    # @abstractmethod
    # def publish_time(self):
    #  """ return publish_time """

    def is_valid(self):
        return True

    def should_update(self, name, version):
        versionDct = self.get_version(name)
        return version > versionDct["version"]

    def update_version(self, name, version):
        versionDct = self.get_version(name)
        if version > versionDct["version"]:
            versionDct["version"] = version
            versionDct["timestamp"] = time.time()
            if "order" not in versionDct:
                versionDct["order"] = len(self.get_versions())
            self.updated = True

    def terminates(self):
        """ set terminated flag for this message """
        self.terminated = True

    def get(self, key, default=None):
        """ get value of key in content """
        return self.dct.get(key, default)

    def update(self, other):
        """ update message content """
        self.dct.update(other)

    def replace(self, other):
        """ replace message content """
        self.dct = other
