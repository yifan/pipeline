import os
import time
import uuid

import redis

from ..cache import parse_connection_string, Cache
from ..tap import SourceTap, DestinationTap


def namespacedTopic(topic, namespace=None):
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class RedisStreamSource(SourceTap):
    """RedisStreamSource reads from Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisStreamSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisStreamSource(config, logger=logging)
    RedisStreamSource(host="localhost:6379",name="in-topic")
    """

    kind = "XREDIS"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = redis.Redis(config.redis)
        self.topic = config.in_topic
        self.group = config.group
        self.name = namespacedTopic(config.in_topic, config.namespace)
        self.timeout = config.timeout
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )
        try:
            self.redis.xgroup_create(self.name, self.group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            logger.error(str(e))

        self.consumer = str(uuid.uuid1())
        self.last_msg = None

    def __repr__(self):
        return 'RedisStreamSource(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--redis",
            type=str,
            default=os.environ.get("REDIS", "localhost:6379"),
            help="redis host:port",
        )
        parser.add_argument(
            "--group",
            type=str,
            default=os.environ.get("GROUP", "group"),
            help="consumer group name",
        )

    def read(self):
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
                    yield self.messageClass.deserialize(
                        data[b"data"], config=self.config
                    )
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        self.logger.info("acknowledged message %s", self.last_msg)
        self.redis.xack(self.topic, self.group, self.last_msg)

    def close(self):
        self.redis.xgroup_delconsumer(self.topic, self.group, self.consumer)
        self.redis.close()


class RedisStreamDestination(DestinationTap):
    """RedisDestination writes to Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisStreamDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisStreamDestination(config, logger=logging)
    RedisStreamDestination(host="localhost:6379",name="out-topic")
    """

    kind = "XREDIS"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = redis.Redis(config.redis)
        self.topic = config.out_topic
        self.name = namespacedTopic(config.out_topic, config.namespace)
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )

    def __repr__(self):
        return 'RedisStreamDestination(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--redis",
            type=str,
            default=os.environ.get("REDIS", "localhost:6379"),
            help="redis host:port",
        )

    def write(self, message):
        serialized = message.serialize()
        self.redis.xadd(self.name, fields={"data": serialized})
        return len(serialized)

    def close(self):
        self.redis.close()


class RedisListSource(SourceTap):
    """RedisListSource reads from Redis Stream

    NOTE: Redis List does not support acknowledgement

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisListSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisListSource(config, logger=logging)
    RedisListSource(host="localhost:6379",name="in-topic")
    """

    kind = "LREDIS"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = redis.Redis(config.redis)
        self.group = config.group
        self.topic = namespacedTopic(config.in_topic, config.namespace)
        self.timeout = config.timeout
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )
        self.last_msg = None

    def __repr__(self):
        return 'RedisListSource(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.topic,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--redis",
            type=str,
            default=os.environ.get("REDIS", "localhost:6379"),
            help="redis host:port",
        )
        parser.add_argument(
            "--group",
            type=str,
            default=os.environ.get("GROUP", "group"),
            help="consumer group name",
        )

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                value = self.redis.lpop(self.topic)
                if value:
                    msg = self.messageClass.deserialize(value, config=self.config)
                    self.logger.info("Read message %s", str(msg))
                    yield msg
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        self.logger.info("Acknowledgement is not supported for LREDIS (Redis List)")

    def close(self):
        self.redis.close()


class RedisListDestination(DestinationTap):
    """RedisListDestination writes to Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisListDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisListDestination(config, logger=logging)
    RedisListDestination(host="localhost:6379",name="out-topic")
    """

    kind = "LREDIS"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = redis.Redis(config.redis)
        self.topic = namespacedTopic(config.out_topic, config.namespace)
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )

    def __repr__(self):
        return 'RedisListDestination(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.topic,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--redis",
            type=str,
            default=os.environ.get("REDIS", "localhost:6379"),
            help="redis host:port",
        )

    def write(self, message):
        serialized = message.serialize()
        self.redis.rpush(self.topic, serialized)
        return len(serialized)

    def close(self):
        self.redis.close()


class RedisCache(Cache):
    """RedisCache reads/writes data from/to Redis

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> RedisCache.add_arguments(parser)
    >>> config = parser.parse_args(["--in-fields", "text,title"])
    >>> with patch('redis.Redis') as c:
    ...     RedisCache(config, logger=logging)
    RedisCache(localhost:6379):['text', 'title']:[]
    """

    kind = "REDIS"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.setup()

    def __repr__(self):
        return "RedisCache({}:{}):{}:{}".format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.in_fields,
            self.out_fields,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--redis",
            type=str,
            default=os.environ.get("REDIS", "localhost:6379"),
            help="redis host:port",
        )
        parser.add_argument(
            "--expire",
            type=int,
            default=os.environ.get("REDISEXPIRE", 7 * 86400),
            help="expire time for database (default: 7 days)",
        )

    def setup(self):
        self.redisConfig = parse_connection_string(self.config.redis, no_username=True)
        self.redis = redis.Redis(
            host=self.redisConfig.host,
            port=self.redisConfig.port,
            password=self.redisConfig.password,
        )

    def read(self, key):
        """entries are stored as following in redis:
        a set is managed for each key to contain fields available
        a key:field -> value for accessing field for each key

        TODO: raise error if fields are not available
        """
        results = self.redis.mget(
            ["{}:{}".format(key, field) for field in self.in_fields]
        )
        return dict(zip(self.fields, results))

    def write(self, key, kvs):
        """entries are stored as following in redis:
        a set is managed for each key to contain fields available
        a key:field -> value for accessing field for each key

        TODO: check error after mset
        """
        self.redis.mset(
            dict([(key, k) for k, v in kvs.items()]),
        )
        for key in kvs.keys():
            self.redis.expire(key, self.config.expire)
