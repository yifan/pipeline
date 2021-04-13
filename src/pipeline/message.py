import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Type, Dict, List, Optional, Union, KeysView
from pydantic import BaseModel, UUID1
import zstandard

from .exception import PipelineMessageError


class Kind(str, Enum):
    Message = "MESG"
    Describe = "DESC"


class Message(BaseModel):
    """Message is a container for data in pipeline. It keeps track of
    information of workers in meta and the content

    Usage:
    >>> msg = Message(id="key", content={"field": "value"})
    >>> serialized = msg.serialize()
    >>> deserialized_msg = Message.deserialize(serialized)
    """

    kind: Optional[Kind] = Kind.Message
    id: Union[UUID1, str] = uuid.uuid1()
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
        if data[0] == ord("{"):
            return cls.parse_raw(data.decode("utf-8"))
        elif data[0] == ord("Z"):
            return cls.parse_raw(cls._decompress(data[1:]).decode("utf-8"))
        else:
            raise PipelineMessageError("Unknown format")

    def serialize(self, compress: bool = False) -> bytes:
        data = self.json().encode("utf-8")
        if compress:
            data = b"Z" + self._compress(data)
        return data

    def as_model(self, model_class: Type[BaseModel]) -> BaseModel:
        return model_class(**self.content)

    def update_content(self, other: BaseModel) -> KeysView[str]:
        d = other.dict()
        self.content.update(d)
        return d.keys()

    def get(self, key: str, default: Any = None) -> Any:
        return self.content.get(key, default)


class DescribeMessage(Message):
    """DescribeMessage is a special message to be sent to worker as a command

    Usage:
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
