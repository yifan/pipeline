#!/usr/bin/env python
import json
import logging
import os
import sys
import time
import uuid
from abc import ABC, abstractmethod

import pika
import pulsar
from confluent_kafka import (
    OFFSET_BEGINNING, Consumer, KafkaError,
    KafkaException, Producer
)
import redis

from .message import Message
from .cache import parse_connection_string

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('worker')
logger.setLevel(logging.DEBUG)


class SourceTap(ABC):
    """ Tap defines the interface for connecting components in pipeline.
    A source will emit Message.
    """
    kind = 'NONE'

    def __init__(self, config, logger=logger):
        self.config = config
        self.rewind = config.rewind
        self.topic = config.in_topic
        self.timeout = config.timeout
        self.logger = logger
        self.messageClass = config.message if hasattr(config, 'message') else Message

    @abstractmethod
    def read(self):
        """receive message."""

    def rewind(self):
        """rewind to earliest message."""
        pass

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--namespace', type=str,
                            default=os.environ.get('NAMESPACE'),
                            help='namespace (default: None)')
        parser.add_argument('--in-topic', type=str,
                            default=os.environ.get('INTOPIC', 'in-topic'),
                            help='topic to read from')
        parser.add_argument('--rewind', action='store_true', default=False,
                            help='read from earliest message')
        parser.add_argument('--timeout', type=int,
                            default=os.environ.get('TIMEOUT', 0),
                            help='request timeout')

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    def close(self):
        pass

    def acknowledge(self):
        pass


class DestinationTap(ABC):
    """ Tap defines the interface for connecting components in pipeline.
    """
    kind = 'NONE'

    def __init__(self, config, logger=logger):
        self.config = config
        self.topic = config.out_topic
        self.logger = logger
        self.messageClass = config.message if hasattr(config, 'message') else Message

    @abstractmethod
    def write(self, message):
        """send message."""

    @classmethod
    def is_cls_of(cls, kind):
        return kind == cls.kind

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--namespace', type=str,
                            default=os.environ.get('NAMESPACE'),
                            help='namespace (default: None)')
        parser.add_argument('--out-topic', type=str, default=os.environ.get('OUTTOPIC', 'out-topic'),
                            help='topic to write')

    def close(self):
        pass


def KindsOfSource():
    return [cls.kind for cls in SourceTap.__subclasses__()]


def SourceOf(typename):
    for cls in SourceTap.__subclasses__():
        if cls.is_cls_of(typename):
            return cls


def DestinationOf(typename):
    for cls in DestinationTap.__subclasses__():
        if cls.is_cls_of(typename):
            return cls


class MemorySource(SourceTap):
    """ MemorySource iterates over a list of dict from 'data' in config.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> data = [{'id':1},{'id':2}]
    >>> config = SimpleNamespace(rewind=False, in_topic='test', data=data, timeout=0)
    >>> [ m.dct for m in MemorySource(config).read() ]
    [{'id': 1}, {'id': 2}]
    """
    kind = 'MEM'

    def read(self):
        for i in self.config.data:
            yield self.messageClass(i)

    def rewind(self):
        pass


class MemoryDestination(DestinationTap):
    """ MemoryDestination stores dicts written in results.
    It is for testing only.

    >>> from types import SimpleNamespace
    >>> d = MemoryDestination(SimpleNamespace(out_topic='test'))
    >>> d.write(Message({"id": 1}))
    >>> d.write(Message({"id": 2}))
    >>> [r.dct for r in d.results]
    [{'id': 1}, {'id': 2}]
    """
    kind = 'MEM'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.results = []

    def write(self, message):
        self.results.append(message)


class FileSource(SourceTap):
    """ FileSource iterates over lines from a input
    text file (utf-8), each line should be a json string for a dict.
    It can be used for integration test for workers.

    >>> import tempfile
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileSource.add_arguments(parser)
    >>> with tempfile.NamedTemporaryFile() as tmpfile:
    ...     tmpfile.write(b'[{ }, {"id": 0}]') and True
    ...     tmpfile.flush()
    ...     config = parser.parse_args("--infile {}".format(tmpfile.name).split())
    ...     fileSource = FileSource(config)
    ...     [m.dct["id"] for m in fileSource.read()]
    True
    [0]
    """
    kind = 'FILE'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        if config.infile == '-':
            self.infile = sys.stdin
        else:
            self.infile = open(config.infile, 'r')
        self.repeat = config.repeat
        self.logger.info('File Source: %s (repeat %d)', config.infile, config.repeat)

    def __repr__(self):
        return 'FileSource("{}")'.format(self.infile.name)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--infile', type=str, required=True,
                            help='input file containing one message in JSON format per line')
        parser.add_argument('--repeat', type=int, default=1,
                            help='repeat input N times')

    def read(self):
        for i in range(0, self.repeat):
            for line in self.infile:
                yield self.messageClass(line)
            if self.repeat > 1:
                self.infile.seek(0, 0)


class FileDestination(DestinationTap):
    """ FileDestination writes items to an output file, one item per line in json format.

    >>> import os, tempfile
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser()
    >>> FileDestination.add_arguments(parser)
    >>> tmpdir = tempfile.mkdtemp()
    >>> outFilename = os.path.join(tmpdir, 'outfile.txt')
    >>> config = parser.parse_args(args=[])
    >>> FileDestination(config)
    FileDestination("out-topic.json")
    >>> config = parser.parse_args("--outfile {}".format(outFilename).split())
    >>> FileDestination(config)
    FileDestination("...outfile.txt")
    """
    kind = 'FILE'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        if config.outfile is None:
            if config.out_topic.find('.') >= 0:
                self.filename = config.out_topic
            else:
                self.filename = config.out_topic + '.json'
        else:
            self.filename = config.outfile

        if config.outfile == '-':
            self.outFile = sys.stdout
        else:
            if config.overwrite:
                self.outFile = open(self.filename, 'w')
            else:
                self.outFile = open(self.filename, 'a')
        self.logger.info('File Destination: %s', self.filename)

    def __repr__(self):
        return 'FileDestination("{}")'.format(self.filename)

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--outfile', type=str,
                            help='output json file')
        parser.add_argument('--overwrite', action='store_true', default=False,
                            help='overwrite output file instead of append')

    def write(self, message):
        print(message.serialize().decode('utf-8'), file=self.outFile, flush=True)

    def close(self):
        if self.filename != '-':
            self.outFile.close()
            self.logger.info('File Destination closed')


class KafkaSource(SourceTap):
    """ KafkaSource reads from KAFKA

    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> KafkaSource.add_arguments(parser)
    >>> config = parser.parse_args(["--config", '{"sasl.mechanisms": "PLAIN"}'])
    >>> KafkaSource(config)
    KafkaSource(host="kafka.kafka.svc.cluster.local",groupid="group-id",topic="in-topic")
    """
    kind = 'KAFKA'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)

        kafkaConfig = {
            'bootstrap.servers': config.kafka,
            'broker.address.family': 'v4',
            'group.id': config.group_id,
            'auto.offset.reset': 'earliest',
            # 'log_level': 0,
            # 'debug': 'consumer',
            'enable.auto.commit': 'false',
        }

        if config.config:
            extraConfig = json.loads(config.config)
            kafkaConfig.update(extraConfig)

        self.consumer = Consumer(kafkaConfig, logger=self.logger)
        self.topic = config.in_topic
        self.last_msg = None

        def maybe_rewind(c, partitions):
            self.logger.info('Assignment: %s', str(partitions))
            if config.rewind:
                for partition in partitions:
                    partition.offset = OFFSET_BEGINNING
                self.logger.info('Rewind, new assignment: %s', str(partitions))
                c.assign(partitions)

        self.consumer.subscribe([self.topic], on_assign=maybe_rewind)
        self.logger.info('KAFKA consumer subscribed to topic %s', self.topic)

    def __repr__(self):
        return 'KafkaSource(host="{}",groupid="{}",topic="{}")'.format(
            self.config.kafka,
            self.config.group_id,
            self.topic
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--kafka', type=str,
                            default=os.environ.get('KAFKA', 'kafka.kafka.svc.cluster.local'),
                            help='kafka address')
        parser.add_argument('--group-id', type=str,
                            default=os.environ.get('GROUPID', 'group-id'),
                            help='group id')
        parser.add_argument('--config', type=str,
                            default=os.environ.get('KAFKACONFIG', None),
                            help='kafka configuration in JSON format')
        parser.add_argument('--poll-timeout', type=int, default=30,
                            help='poll new message timeout in seconds')

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            msg = self.consumer.poll(timeout=self.config.poll_timeout)
            if msg is None:
                self.logger.warning('No message to read, timed out')
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.warning('Reaching EOF')
                    break
                else:
                    raise KafkaException(msg.error())
            else:
                self.logger.info('Read {}, {}'.format(msg.topic(), msg.offset()))
                self.last_msg = msg
                yield self.messageClass(msg.value())
                lastMessageTime = time.time()
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        if self.last_msg:
            self.consumer.commit(message=self.last_msg)

    def close(self):
        self.consumer.close()


class KafkaDestination(DestinationTap):
    """ KafkaDestination writes to KAFKA

    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> KafkaDestination.add_arguments(parser)
    >>> config = parser.parse_args(["--config", '{"sasl.mechanisms": "PLAIN"}'])
    >>> KafkaDestination(config)
    KafkaDestination(host="kafka.kafka.svc.cluster.local",topic="out-topic")
    """
    kind = 'KAFKA'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        kafkaConfig = {
            'bootstrap.servers': config.kafka,
            'queue.buffering.max.ms': 100,
            'message.send.max.retries': 5,
            'request.required.acks': 'all',
            'broker.address.family': 'v4',
        }

        if config.config:
            extraConfig = json.loads(config.config)
            kafkaConfig.update(extraConfig)

        self.topic = config.out_topic
        self.producer = Producer(kafkaConfig, logger=self.logger)

    def __repr__(self):
        return 'KafkaDestination(host="{}",topic="{}")'.format(
            self.config.kafka,
            self.topic
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--kafka', type=str,
                            default=os.environ.get('KAFKA', 'kafka.kafka.svc.cluster.local'),
                            help='kafka address')
        parser.add_argument('--config', type=str,
                            default=os.environ.get('KAFKACONFIG', None),
                            help='kafka configuration in JSON format')

    def write(self, message):
        def delivery_report(err, msg):
            if err:
                self.logger.error('Message delivery failed ({} [{}]: {}'.format(
                    msg.topic(),
                    str(msg.partition()),
                    err
                ))
            else:
                self.logger.info('Message delivered to {} [{}] {}'.format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                ))
        self.producer.produce(self.topic, message.serialize(), callback=delivery_report)
        self.producer.flush()

    def close(self):
        self.producer.flush()


class PulsarSource(SourceTap):
    """ PulsarSource reads from Pulsar

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> PulsarSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pulsar.Client') as c:
    ...     PulsarSource(config)
    PulsarSource(host="pulsar://pulsar.pulsar.svc.cluster.local:6650",name="persistent://meganews/test/in-topic",subscription="subscription")
    """
    kind = 'PULSAR'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.config = config
        self.client = pulsar.Client(config.pulsar)
        self.tenant = config.tenant
        self.namespace = config.namespace
        self.topic = config.in_topic
        self.subscription = config.subscription
        self.name = 'persistent://{}/{}/{}'.format(
            self.tenant,
            self.namespace,
            self.topic
        )
        self.consumer = self.client.subscribe(
            self.name,
            self.subscription,
            receiver_queue_size=1,
            consumer_type=pulsar.ConsumerType.Shared,
        )
        self.last_msg = None

    def __repr__(self):
        return 'PulsarSource(host="{}",name="{}",subscription="{}")'.format(
            self.config.pulsar,
            self.name,
            self.subscription,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--pulsar', type=str,
                            default=os.environ.get('PULSAR', 'pulsar://pulsar.pulsar.svc.cluster.local:6650'),
                            help='pulsar address')
        parser.add_argument('--tenant', type=str,
                            default=os.environ.get('TENANT', 'meganews'),
                            help='pulsar tenant, always is meganews')
        parser.add_argument('--namespace', type=str,
                            default=os.environ.get('NAMESPACE', 'test'),
                            help='pulsar namespace (default: test')
        parser.add_argument('--subscription', type=str,
                            default=os.environ.get('SUBSCRIPTION', 'subscription'),
                            help='subscription to read')

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                msg = self.consumer.receive()
                self.last_msg = msg
                yield self.messageClass(msg.data())
                lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        self.consumer.acknowledge(self.last_msg)

    def close(self):
        self.client.close()


class PulsarDestination(DestinationTap):
    """ PulsarDestination writes to Pulsar

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> PulsarDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pulsar.Client') as c:
    ...     PulsarDestination(config)
    PulsarDestination(host="pulsar://pulsar.pulsar.svc.cluster.local:6650",name="persistent://meganews/test/out-topic")
    """
    kind = 'PULSAR'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.config = config
        self.client = pulsar.Client(config.pulsar)
        self.tenant = config.tenant
        self.namespace = config.namespace
        self.topic = config.out_topic
        self.name = 'persistent://{}/{}/{}'.format(
            self.tenant, self.namespace, self.topic)
        self.producer = self.client.create_producer(self.name)

    def __repr__(self):
        return 'PulsarDestination(host="{}",name="{}")'.format(
            self.config.pulsar,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--pulsar', type=str,
                            default=os.environ.get('PULSAR', 'pulsar://pulsar.pulsar.svc.cluster.local:6650'),
                            help='pulsar address')
        parser.add_argument('--tenant', type=str,
                            default=os.environ.get('TENANT', 'meganews'),
                            help='pulsar tenant, always is meganews')
        parser.add_argument('--namespace', type=str,
                            default=os.environ.get('NAMESPACE', 'test'),
                            help='pulsar namespace (default: test)')

    def write(self, message):
        self.producer.send(message.serialize())

    def close(self):
        self.client.close()


def namespacedTopic(topic, namespace=None):
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class RedisStreamSource(SourceTap):
    """ RedisStreamSource reads from Redis Stream

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisStreamSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisStreamSource(config)
    RedisStreamSource(host="localhost:6379",name="in-topic")
    """
    kind = 'XREDIS'

    def __init__(self, config, logger=logger):
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
            self.redis.xgroup_create(self.name, self.group, id=u'0', mkstream=True)
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
        parser.add_argument('--redis', type=str,
                            default=os.environ.get('REDIS', 'localhost:6379'),
                            help='redis host:port')
        parser.add_argument('--group', type=str,
                            default=os.environ.get('GROUP', 'group'),
                            help='consumer group name')

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                msg = self.redis.xreadgroup(self.group, self.consumer, {self.topic: '>'}, count=1)
                if msg:
                    (msgId, data) = msg[0][1][0]
                    self.last_msg = msgId
                    self.logger.info('Read message %s', msgId)
                    yield self.messageClass(data[b'data'])
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        self.logger.info('acknowledged message %s', self.last_msg)
        self.redis.xack(self.topic, self.group, self.last_msg)

    def close(self):
        self.redis.xgroup_delconsumer(self.topic, self.group, self.consumer)
        self.redis.close()


class RedisStreamDestination(DestinationTap):
    """ RedisDestination writes to Redis Stream

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisStreamDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisStreamDestination(config)
    RedisStreamDestination(host="localhost:6379",name="out-topic")
    """
    kind = 'XREDIS'

    def __init__(self, config, logger=logger):
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
        parser.add_argument('--redis', type=str,
                            default=os.environ.get('REDIS', 'localhost:6379'),
                            help='redis host:port')

    def write(self, message):
        self.redis.xadd(self.name, fields={'data': message.serialize()})

    def close(self):
        self.redis.close()


class RabbitMQSource(SourceTap):
    """ RabbitMQSource reads from RabbitMQ

    RabbitMQ options:
        --rabbitmq (env: RABBITMQ): RabbitMQ host

    Source options:
        --in-topic (env: INTOPIC): queue to read from
        --timeout (env: TIMEOUT): seconds to exit if no new messages

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RabbitMQSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pika.ConnectionParameters') as c1:
    ...     with patch('pika.BlockingConnection') as c2:
    ...         RabbitMQSource(config)
    RabbitMQSource(queue="in-topic")
    """
    kind = 'RABBITMQ'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.config = config
        self.topic = config.in_topic
        self.name = namespacedTopic(config.in_topic, config.namespace)
        self.timeout = config.timeout
        parameters = pika.ConnectionParameters(config.rabbitmq)
        self.rabbit = pika.BlockingConnection(parameters)
        self.channel = self.rabbit.channel()
        self.channel.queue_declare(queue=self.name)
        self.delivery_tag = None
        self.msg = None
        self.logger.info('RabbitMQSource initialized.')

    def __repr__(self):
        return 'RabbitMQSource(queue="{}")'.format(
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--rabbitmq', type=str,
                            default=os.environ.get('RABBITMQ', 'localhost'),
                            help='RabbitMQ host')

    def read(self):
        timedOut = False
        lastMessageTime = time.time()

        while not timedOut:
            try:
                method, header, body = self.channel.basic_get(self.name)
            except pika.exceptions.AMQPConnectionError:
                self.logger.warning('Trying to restore connection to RabbitMQ...')
                parameters = pika.ConnectionParameters(self.config.rabbitmq)
                self.rabbit = pika.BlockingConnection(parameters)
                self.channel = self.rabbit.channel()
                self.logger.warning('Connection to RabbitMQ restored.')
                method, header, body = self.channel.basic_get(self.name)
            except Exception as ex:
                self.logger.error(ex)
                break

            if method:
                self.delivery_tag = method.delivery_tag
                self.logger.info('Read message %s', self.delivery_tag)
                yield self.messageClass(body)
                lastMessageTime = time.time()
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                self.logger.info('RabbitMQSource timed out.')
                timedOut = True

    def acknowledge(self):
        self.logger.info('acknowledged message %s', self.delivery_tag)
        self.channel.basic_ack(self.delivery_tag)

    def close(self):
        self.logger.info('RabbitMQSource closed.')
        self.channel.close()
        self.rabbit.close()


class RabbitMQDestination(DestinationTap):
    """ RabbitMQDestination writes to RabbitMQ

    options:
        --rabbitmq (env: RABBITMQ): RabbitMQ host

    standard options:
        --out-topic (env: OUTTOPIC): queue to write to

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RabbitMQDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pika.ConnectionParameters') as c1:
    ...     with patch('pika.BlockingConnection') as c2:
    ...         RabbitMQDestination(config)
    RabbitMQDestination(queue="out-topic")
    """
    kind = 'RABBITMQ'

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.config = config
        self.topic = config.out_topic
        self.name = namespacedTopic(config.out_topic, config.namespace)
        parameters = pika.ConnectionParameters(config.rabbitmq, heartbeat=5)
        self.rabbit = pika.BlockingConnection(parameters)
        self.channel = self.rabbit.channel()
        self.channel.queue_declare(queue=self.name)
        self.logger.info('RabbitMQDestination initialized.')

    def __repr__(self):
        return 'RabbitMQDestination(queue="{}")'.format(
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--rabbitmq', type=str,
                            default=os.environ.get('RABBITMQ', 'localhost'),
                            help='RabbitMQ host')

    def write(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.name, body=message.serialize())
            return
        except pika.exceptions.StreamLostError:
            self.logger.warning('Trying to restore connection to RabbitMQ...')
            self.rabbit = pika.BlockingConnection(pika.ConnectionParameters(self.config.rabbitmq))
            self.channel = self.rabbit.channel()
            self.logger.warning('Connection to RabbitMQ restored.')
            self.channel.basic_publish(exchange='', routing_key=self.name, body=message.serialize())

    def close(self):
        self.logger.info('RabbitMQDestination closed.')
        self.channel.close()
        self.rabbit.close()


class RedisListSource(SourceTap):
    """ RedisListSource reads from Redis Stream

    NOTE: Redis List does not support acknowledgement

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisListSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisListSource(config)
    RedisListSource(host="localhost:6379",name="in-topic")
    """
    kind = 'LREDIS'

    def __init__(self, config, logger=logger):
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
        self.last_msg = None

    def __repr__(self):
        return 'RedisListSource(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--redis', type=str,
                            default=os.environ.get('REDIS', 'localhost:6379'),
                            help='redis host:port')
        parser.add_argument('--group', type=str,
                            default=os.environ.get('GROUP', 'group'),
                            help='consumer group name')

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                value = self.redis.lpop(self.topic)
                if value:
                    msg = self.messageClass(value)
                    self.logger.info('Read message %s', str(msg))
                    yield msg
                    lastMessageTime = time.time()
            except Exception as ex:
                self.logger.error(ex)
                break
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        self.logger.info('Acknowledgement is not supported for LREDIS (Redis List)')

    def close(self):
        self.redis.close()


class RedisListDestination(DestinationTap):
    """ RedisListDestination writes to Redis Stream

    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> RedisListDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('redis.Redis') as c:
    ...     RedisListDestination(config)
    RedisListDestination(host="localhost:6379",name="out-topic")
    """
    kind = 'LREDIS'

    def __init__(self, config, logger=logger):
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
        return 'RedisListDestination(host="{}:{}",name="{}")'.format(
            self.redisConfig.host,
            self.redisConfig.port,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument('--redis', type=str,
                            default=os.environ.get('REDIS', 'localhost:6379'),
                            help='redis host:port')

    def write(self, message):
        self.redis.rpush(self.name, message.serialize())

    def close(self):
        self.redis.close()
