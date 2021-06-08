import uuid
import json
from datetime import datetime
from enum import Enum
from typing import Any, Type, Dict, List, Optional, KeysView, Set, Union
from pydantic import BaseModel, parse_obj_as
import zstandard

from .exception import PipelineMessageError


class Kind(str, Enum):
    Message = "MESG"
    Describe = "DESC"


class Log(BaseModel):
    name: str
    version: str
    updated: Set[str]
    received: datetime = datetime.now()
    processed: Optional[datetime] = None
    elapsed: Optional[float] = None


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
    id: str = uuid.uuid1().hex
    created: datetime = datetime.now()
    logs: List[Log] = []
    content: Dict[str, Any] = {}

    def serialize(self, compress: bool = False) -> bytes:
        return serialize_message(self, compress)

    @classmethod
    def deserialize(self, raw: bytes) -> "Message":
        return deserialize_message(raw)

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

    input_schema: Optional[str] = None
    output_schema: Optional[str] = None
    kind: Kind = Kind.Describe


def serialize_message(message: BaseModel, compress: bool = False) -> bytes:
    data = message.json().encode("utf-8")
    if compress:
        data = b"Z" + zstandard.compress(data)
    return data


def deserialize_message(raw: bytes) -> Union[Message, DescribeMessage]:
    if raw[0] == ord("{"):
        message_dict = json.loads(raw.decode("utf-8"))
    elif raw[0] == ord("Z"):
        message_dict = json.loads(zstandard.decompress(raw[1:]).decode("utf-8"))
    else:
        raise PipelineMessageError("Unknown bytes string cannot be deserialized")

    if message_dict["kind"] == Kind.Message:
        return parse_obj_as(Message, message_dict)
    elif message_dict["kind"] == Kind.Describe:
        return parse_obj_as(DescribeMessage, message_dict)
    else:
        raise PipelineMessageError("Unknown format")
