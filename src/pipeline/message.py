import json
from abc import ABC
import time
import traceback

from .exception import PipelineError


class Message(ABC):
    keyname = 'key'

    def __init__(self, other=None):
        self.updated = False
        self.terminated = False
        self.info = {}
        self.dct = {}
        try:
            if other is not None:
                if isinstance(other, type(self)):
                    self.info = other.info
                    self.dct = other.dct
                elif isinstance(other, dict):
                    self.dct = other
                elif isinstance(other, bytes):
                    [self.info, self.dct] = self.deserialize(other)
                elif isinstance(other, str):
                    [self.info, self.dct] = self.deserialize(other)
                else:
                    raise PipelineError(
                        'Message needs to be initialized with a message, a dict or str/bytes, not "{}"'
                        .format(type(other)),
                        data=other
                    )
        except PipelineError as error:
            raise error
        except Exception as error:
            raise PipelineError(
                str(error), data=other, traceback=traceback.print_exception()
            )

    def __str__(self):
        return '{}<{}:{}>'.format(type(self).__name__, self.keyname, self.dct.get(self.keyname, None))

    def __repr__(self):
        return self.__str__()

    def __unicode__(self):
        return self.__str__()

    def log(self, logger):
        logger.warning(self.log_info)
        logger.warning(self.log_content)

    def log_info(self):
        return json.dumps(self.info, indent=4)

    def log_content(self):
        return json.dumps(self.dct, indent=4)

    @classmethod
    def add_arguments(cls, parser):
        return parser

    def serialize(self, indent=None):
        """serialize to binary."""
        return json.dumps([self.info, self.dct], indent=indent).encode('utf-8')

    @classmethod
    def deserialize(cls, raw):
        """deserialize to json."""
        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')
        return json.loads(raw)

    def get_version(self, name):
        return self.info.setdefault(name, {'version': [], })

    def get_versions(self):
        return self.info

    # @abstractmethod
    # def publish_time(self):
    #  """ return publish_time """

    def is_valid(self):
        return True

    def should_update(self, name, version):
        versionDct = self.get_version(name)
        return version > versionDct['version']

    def update_version(self, name, version):
        versionDct = self.get_version(name)
        if version > versionDct['version']:
            versionDct['version'] = version
            versionDct['timestamp'] = time.time()
            if 'order' not in versionDct:
                versionDct['order'] = len(self.get_versions())
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
