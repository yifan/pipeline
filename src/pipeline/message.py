import json
import os
import zlib
from abc import ABC
import time


class Message(ABC):
    """Message is a container for data flowing in pipeline. To construct a message,
    you need to pass a dict in Message constructor. Message is expected to have a
    key (the name is defined in Message.keyname) in this dict

    Usage:
    >>> message = Message({'key': 'unique-id-1', 'data': 'data'})
    """

    keyname = "key"

    def __init__(self, dct, header=None, config=None):
        self.config = config
        self.updated = False
        self.terminated = False
        if header is None:
            self.header = {}
        else:
            self.header = header
        self.dct = dct

    def __str__(self):
        return "{}<{}:{}>".format(
            type(self).__name__, self.keyname, self.dct.get(self.keyname, None)
        )

    def __repr__(self):
        return self.__str__()

    def __unicode__(self):
        return self.__str__()

    @classmethod
    def _compress(cls, data):
        return zlib.compress(json.dumps(data).encode("utf-8"))

    @classmethod
    def _decompress(cls, data):
        return json.loads(zlib.decompress(data))

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
        parser.add_argument(
            "--compress",
            default=os.environ.get('COMPRESS', 'FALSE') == 'TRUE',
            action="store_true",
            help="zlib compress content",
        )
        return parser

    def serialize(self, indent=None):
        """serialize to binary."""
        data = json.dumps([self.header, self.dct], indent=indent).encode(
            "utf-8"
        )
        if getattr(self.config, "compress", False) is True:
            return zlib.compress(data)
        else:
            return data

    @classmethod
    def deserialize(cls, raw, config=None):
        """deserialize to json."""
        if raw[0] == ord("{"):
            header = {}
            content = json.loads(raw)
        elif raw[0] == ord("["):
            data = raw
            header, content = json.loads(data)
        else:
            data = zlib.decompress(raw)
            header, content = json.loads(data)
        return cls(content, header=header, config=config)

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
