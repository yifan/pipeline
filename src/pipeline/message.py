import json
import os
import time
import zlib
from abc import ABC


class Message(ABC):
    """Message is a container for data flowing in pipeline. To construct a message,
    you need to pass a dict in Message constructor. Message is expected to have a
    key (the name is defined in Message.keyname) in this dict

    Usage:
    >>> message = Message({'key': 'unique-id-1', 'data': 'data'})
    """

    version = 1
    keyname = "key"

    def __init__(self, dct, header=None, config=None):
        self.config = config
        self.updated = False
        self.terminated = False
        if header is None:
            self.header = {
                "version": self.version,
                "created": time.time(),
                "history": [],
            }
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
        logger.debug(self.log_header())
        logger.debug(self.log_content())

    def log_info(self):
        """ for compatibility only """
        return self.log_header()

    def log_header(self):
        return json.dumps(self.header, indent=4)

    def log_content(self):
        return json.dumps(self.dct, indent=4)[:1024]

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            "--compress",
            default=os.environ.get("COMPRESS", "FALSE") == "TRUE",
            action="store_true",
            help="zlib compress content",
        )
        return parser

    def serialize(self, indent=None, no_compress=False):
        """serialize to binary."""
        data = json.dumps([self.header, self.dct], indent=indent).encode("utf-8")

        if getattr(self.config, "compress", False) is True and no_compress is False:
            return zlib.compress(data)
        else:
            return data

    @classmethod
    def deserialize(cls, raw, config=None):
        """deserialize to json."""
        if raw[0] == ord("{"):
            header = None
            content = json.loads(raw)
        elif raw[0] == ord("["):
            data = raw
            header, content = json.loads(data)
        else:
            data = zlib.decompress(raw)
            header, content = json.loads(data)
        return cls(content, header=header, config=config)

    def get_version(self, name):
        for version in self.header["history"]:
            if version["name"] == name:
                return version

    def get_versions(self):
        return self.header

    # @abstractmethod
    # def publish_time(self):
    #  """ return publish_time """

    def is_valid(self):
        return True

    def should_update(self, name, version):
        versionDct = self.get_version(name)
        return versionDct is None or version > versionDct["version"]

    def update_version(self, name, version):
        self.header["history"].insert(
            0,
            {
                "name": name,
                "version": version,
                "receive": time.time(),
            },
        )
        self.updated = True

    def complete(self):
        self.header["history"][0]["complete"] = time.time()

    def terminates(self):
        """ set terminated flag for this message """
        self.terminated = True

    def key(self):
        """ message id/key is defined by Message.keyname """
        try:
            return self.dct[self.keyname]
        except KeyError:
            raise KeyError(f"Key {self.keyname} is missing in message")

    def get(self, key, default=None):
        """ get value of key in content """
        return self.dct.get(key, default)

    def update(self, other):
        """ update message content """
        self.dct.update(other)
        # keep track of updated fields
        self.header["updated_fields"] = list(other.keys())

    def get_updates(self):
        """ return a dict containing last updated fields """
        keys = self.header.get("updated_fields", [])
        return {k: self.dct[k] for k in keys}

    def replace(self, other):
        """ replace message content """
        self.dct = other
