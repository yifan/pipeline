import time
import uuid
import logging
from logging import Logger
from typing import Iterator, ClassVar, Any

from pydantic import Field
from redis import Redis, ResponseError as RedisResponseError

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase
from ..helpers import namespaced_topic


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
pipelineLogger = logging.getLogger("pipeline")
pipelineLogger.setLevel(logging.DEBUG)


class RedisSourceSettings(SourceSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")
    group: str = Field(None, title="redis consumer group name")
    min_idle_time: int = Field(
        3600000,
        title="messages not acknowledged after min-idle-time will be reprocessed",
    )


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

    def _create_group(self):
        try:
            self.redis.xgroup_create(self.topic, self.group, id="0", mkstream=True)
        except RedisResponseError as e:
            self.logger.error(str(e))
            raise

    def length(self) -> int:
        return self.redis.xlen(self.topic)

    def read(self) -> Iterator[MessageBase]:
        self._create_group()

        timedOut = False
        last_message_time = time.time()

        while not timedOut:
            try:
                # claim lost pending messages
                msg = self.redis.xautoclaim(
                    self.group, self.consumer, self.settings.min_idle_time, "0", count=1
                )

                if msg:
                    (msgId, data) = msg[0]
                else:
                    # if no lost pending message, read new message
                    msg = self.redis.xreadgroup(
                        self.group, self.consumer, {self.topic: ">"}, count=1
                    )
                    if msg:
                        (msgId, data) = msg[0][1][0]
                    else:  # no message
                        data = None

                # for either old or new message
                if data:
                    self.last_msg = msgId
                    self.logger.info("Read message %s", msgId)
                    yield MessageBase.deserialize(data[b"data"])
                    last_message_time = time.time()

            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0:
                elapsed_time = time.time() - last_message_time
                if elapsed_time > self.timeout:
                    self.logger.warning(
                        "reader timed out after %d seconds (timeout %d)",
                        elapsed_time,
                        self.timeout,
                    )
                    timedOut = True

    def acknowledge(self) -> None:
        self.logger.info("acknowledged message %s", self.last_msg)
        self.redis.xack(self.topic, self.group, self.last_msg)

    def close(self) -> None:
        self.redis.xgroup_delconsumer(self.topic, self.group, self.consumer)
        self.redis.close()


class RedisDestinationSettings(DestinationSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")
    maxlen: int = Field(1, title="maximum length for stream")


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

    def length(self) -> int:
        return self.redis.xlen(self.topic)

    def write(self, message: MessageBase) -> int:
        serialized = message.serialize(compress=self.settings.compress)
        self.redis.xadd(
            self.topic, fields={"data": serialized}, maxlen=self.settings.maxlen
        )
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

    def length(self) -> int:
        return self.redis.llen(self.topic)

    def read(self) -> Iterator[MessageBase]:
        timedOut = False
        last_message_time = time.time()
        while not timedOut:
            try:
                value = self.redis.lpop(self.topic)
                if value:
                    msg = MessageBase.deserialize(value)
                    self.logger.info("Read message %s", str(msg))
                    yield msg
                    last_message_time = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0:
                elapsed_time = time.time() - last_message_time
                if elapsed_time > self.timeout:
                    self.logger.warning(
                        "reader timed out after %d seconds (timeout %d)",
                        elapsed_time,
                        self.timeout,
                    )
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

    def length(self) -> int:
        return self.redis.llen(self.topic)

    def write(self, message: MessageBase) -> int:
        serialized = message.serialize(compress=self.settings.compress)
        self.redis.rpush(self.topic, serialized)
        return len(serialized)

    def close(self) -> None:
        self.redis.close()
