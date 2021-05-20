import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Type, Dict, List, Optional, KeysView
from pydantic import BaseModel
import zstandard

from .exception import PipelineMessageError


class Kind(str, Enum):
    Message = "MESG"
    Describe = "DESC"


class Message(BaseModel):
    """:class:`Message` is a container for data in pipeline. Data will be wrapped in
    :class:`Message` in order to pass through a pipeline constructed with this library.

    Usage:

    .. code-block:: python

        >>> msg = Message(id="key", content={"field": "value"})
        >>> serialized = msg.serialize()
        >>> deserialized_msg = Message.deserialize(serialized)
    """

    kind: Optional[Kind] = Kind.Message
    id: str = uuid.uuid1()
    created: datetime = datetime.now()
    logs: List[BaseModel] = []
    content: Dict[str, Any] = {}

    @classmethod
    def _compress(cls, data: bytes) -> bytes:
        return zstandard.compress(data)

    @classmethod
    def _decompress(cls, data: bytes) -> bytes:
        return zstandard.decompress(data)

    @classmethod
    def deserialize(cls, data: bytes) -> "Message":
        """de-serialize message

        Parameters:
            :param data: serialized message
            :type data: bytes
            :return: a new :class:`Message` object
            :rtype: Message
        """
        if data[0] == ord("{"):
            return cls.parse_raw(data.decode("utf-8"))
        elif data[0] == ord("Z"):
            return cls.parse_raw(cls._decompress(data[1:]).decode("utf-8"))
        else:
            raise PipelineMessageError("Unknown format")

    def serialize(self, compress: bool = False) -> bytes:
        """serialize message with optional compression

        Parameters:
            :param compress: compression flag
            :type compress: bool
            :return: serialized message bytes
            :rtype: bytes
        """
        data = self.json().encode("utf-8")
        if compress:
            data = b"Z" + self._compress(data)
        return data

    def as_model(self, model_class: Type[BaseModel]) -> BaseModel:
        """return content as another BaseModel instance

        Parameters:
            :param model_class: return class type
            :type model_class: class
            :return: BaseModel
            :rtype: BaseModel
        """
        return model_class(**self.content)

    def update_content(self, other: BaseModel) -> KeysView[str]:
        """add fields from other model to update message's content

        Parameters:
            :param other: other BaseModel object
            :type other: BaseModel
            :return: list of keys updated
            :rtype: KeysView[str]
        """

        d = other.dict()
        self.content.update(d)
        return d.keys()

    def get(self, key: str, default: Any = None) -> Any:
        """access any field in message content

        Parameters:
            :param key: field name
            :type key: str
            :return: value
            :rtype: Any
        """
        return self.content.get(key, default)


class DescribeMessage(Message):
    """:class:`DescribeMessage` is a special message to be sent to worker as a command

    Usage:

     .. code-block:: python

        >>> describe = DescribeMessage()
        >>> describe.kind == Kind.Describe
        True

    """

    input_schema: Optional[str]
    output_schema: Optional[str]

    def __init__(self) -> None:
        super().__init__(kind=Kind.Describe)
        self.input_schema = None
        self.output_schema = None
