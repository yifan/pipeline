import time
import uuid
from logging import Logger
from typing import Iterator, ClassVar, Any

from pydantic import RedisDsn, Field
from redis import Redis, ResponseError as RedisResponseError

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import Message


def namespacedTopic(topic: str, namespace: str = None) -> str:
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class RedisSourceSettings(SourceSettings):
    redis: RedisDsn = Field("redis://localhost:6379", title="redis url")
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

    def __init__(self, settings: RedisSourceSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.group = settings.group
        self.topic = namespacedTopic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        self.redis = Redis(
            host=self.settings.redis.host,
            port=int(self.settings.redis.port) if self.settings.redis.port else 6380,
            password=self.settings.redis.password,
        )

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
    redis: RedisDsn = Field("redis://localhost:6379", title="redis url")


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

    def __init__(self, settings: RedisDestinationSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.topic = namespacedTopic(settings.topic, settings.namespace)
        self.redis = Redis(
            host=self.settings.redis.host,
            port=int(self.settings.redis.port) if self.settings.redis.port else 6380,
            password=self.settings.redis.password,
        )

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

    def __init__(self, settings: RedisSourceSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.group = settings.group
        self.topic = namespacedTopic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        self.redis = Redis(
            host=settings.redis.host,
            port=int(self.settings.redis.port) if self.settings.redis.port else 6380,
            password=settings.redis.password,
        )
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

    def __init__(self, settings: RedisDestinationSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = Redis(settings.redis)
        self.topic = namespacedTopic(settings.topic, settings.namespace)
        self.redis = Redis(
            host=settings.redis.host,
            port=int(self.settings.redis.port) if self.settings.redis.port else 6380,
            password=settings.redis.password,
        )

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


# class RedisCache(Cache):
#     """RedisCache reads/writes data from/to Redis
#
#     >>> import logging
#     >>> from unittest.mock import patch
#     >>> from argparse import ArgumentParser
#     >>> parser = ArgumentParser()
#     >>> RedisCache.add_arguments(parser)
#     >>> config = parser.parse_args(["--in-fields", "text,title"])
#     >>> with patch('Redis') as c:
#     ...     RedisCache(config, logger=logging)
#     RedisCache(localhost:6379):['text', 'title']:[]
#     """
#
#     kind = "REDIS"
#
#     def __init__(self, config, logger):
#         super().__init__(config, logger)
#         self.setup()
#
#     def __repr__(self):
#         return "RedisCache({}:{}):{}:{}".format(
#             self.redisConfig.host,
#             self.redisConfig.port,
#             self.in_fields,
#             self.out_fields,
#         )
#
#     @classmethod
#     def add_arguments(cls, parser):
#         super().add_arguments(parser)
#         parser.add_argument(
#             "--redis",
#             type=str,
#             default=os.environ.get("REDIS", "localhost:6379"),
#             help="redis host:port",
#         )
#         parser.add_argument(
#             "--expire",
#             type=int,
#             default=os.environ.get("REDISEXPIRE", 7 * 86400),
#             help="expire time for database (default: 7 days)",
#         )
#
#     def setup(self):
#         self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
#         self.redis = Redis(
#             host=self.redisConfig.host,
#             port=self.redisConfig.port,
#             password=self.redisConfig.password,
#         )
#
#     def read(self, key):
#         """entries are stored as following in redis:
#         a set is managed for each key to contain fields available
#         a key:field -> value for accessing field for each key
#
#         TODO: raise error if fields are not available
#         """
#         results = self.redis.mget(
#             ["{}:{}".format(key, field) for field in self.in_fields]
#         )
#         return dict(zip(self.fields, results))
#
#     def write(self, key, kvs):
#         """entries are stored as following in redis:
#         a set is managed for each key to contain fields available
#         a key:field -> value for accessing field for each key
#
#         TODO: check error after mset
#         """
#         self.redis.mset(
#             dict([(key, k) for k, v in kvs.items()]),
#         )
#         for key in kvs.keys():
#             self.redis.expire(key, self.config.expire)
