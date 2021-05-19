import time
import uuid
import logging
from logging import Logger
from typing import Iterator, ClassVar, Any

from pydantic import Field
from redis import Redis, ResponseError as RedisResponseError

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import Message
from ..helpers import namespaced_topic


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


class RedisSourceSettings(SourceSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")
    group: str = Field(None, title="redis consumer group name")


class RedisStreamSource(SourceTap):
    """RedisStreamSource reads from Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RedisSourceSettings()
    >>> RedisStreamSource(settings=settings, logger=logging)
    RedisStreamSource(host="redis://localhost:6379/0", topic="in-topic")
    """

    settings: RedisSourceSettings
    redis: Any

    kind: ClassVar[str] = "XREDIS"

    def __init__(
        self, settings: RedisSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.group = settings.group
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        self.redis = Redis.from_url(settings.redis)
        self.consumer = str(uuid.uuid1())
        self.last_msg = None

    def __repr__(self) -> str:
        return f'RedisStreamSource(host="{self.settings.redis}", topic="{self.topic}")'

    def read(self) -> Iterator[Message]:

        try:
            self.redis.xgroup_create(self.topic, self.group, id="0", mkstream=True)
        except RedisResponseError as e:
            self.logger.error(str(e))
            raise

        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                msg = self.redis.xreadgroup(
                    self.group, self.consumer, {self.topic: ">"}, count=1
                )
                if msg:
                    (msgId, data) = msg[0][1][0]
                    self.last_msg = msgId
                    self.logger.info("Read message %s", msgId)
                    yield Message.deserialize(data[b"data"])
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self) -> None:
        self.logger.info("acknowledged message %s", self.last_msg)
        self.redis.xack(self.topic, self.group, self.last_msg)

    def close(self) -> None:
        self.redis.xgroup_delconsumer(self.topic, self.group, self.consumer)
        self.redis.close()


class RedisDestinationSettings(DestinationSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")


class RedisStreamDestination(DestinationTap):
    """RedisDestination writes to Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RedisDestinationSettings()
    >>> RedisStreamDestination(settings=settings, logger=logging)
    RedisStreamDestination(host="redis://localhost:6379/0", topic="out-topic")
    """

    settings: RedisDestinationSettings
    redis: Any

    kind: ClassVar[str] = "XREDIS"

    def __init__(
        self, settings: RedisDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.redis = Redis.from_url(settings.redis)

    def __repr__(self) -> str:
        return f'RedisStreamDestination(host="{self.settings.redis}", topic="{self.topic}")'

    def write(self, message: Message) -> int:
        serialized = message.serialize(compress=self.settings.compress)
        self.redis.xadd(self.topic, fields={"data": serialized})
        return len(serialized)

    def close(self) -> None:
        self.redis.close()


class RedisListSource(SourceTap):
    """RedisListSource reads from Redis Stream

    NOTE: Redis List does not support acknowledgement

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RedisSourceSettings()
    >>> RedisListSource(settings=settings, logger=logging)
    RedisListSource(host="redis://localhost:6379/0", topic="in-topic")
    """

    settings: RedisSourceSettings
    redis: Any

    kind: ClassVar[str] = "LREDIS"

    def __init__(
        self, settings: RedisSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.group = settings.group
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        self.redis = Redis.from_url(settings.redis)
        self.last_msg = None

    def __repr__(self) -> str:
        return f'RedisListSource(host="{self.settings.redis}", topic="{self.topic}")'

    def read(self) -> Iterator[Message]:
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                value = self.redis.lpop(self.topic)
                if value:
                    msg = Message.deserialize(value)
                    self.logger.info("Read message %s", str(msg))
                    yield msg
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self) -> None:
        self.logger.info("Acknowledgement is not supported for LREDIS (Redis List)")

    def close(self) -> None:
        self.redis.close()


class RedisListDestination(DestinationTap):
    """RedisListDestination writes to Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RedisDestinationSettings()
    >>> RedisListDestination(settings=settings, logger=logging)
    RedisListDestination(host="redis://localhost:6379/0", topic="out-topic")
    """

    settings: RedisDestinationSettings
    redis: Any

    kind: ClassVar[str] = "LREDIS"

    def __init__(
        self, settings: RedisDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.redis = Redis.from_url(settings.redis)

    def __repr__(self) -> str:
        return (
            f'RedisListDestination(host="{self.settings.redis}", topic="{self.topic}")'
        )

    def write(self, message: Message) -> int:
        serialized = message.serialize(compress=self.settings.compress)
        self.redis.rpush(self.topic, serialized)
        return len(serialized)

    def close(self) -> None:
        self.redis.close()
