import logging
import os
import time

import pika

from ..tap import SourceTap, DestinationTap


FORMAT = "%(asctime)-15s %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("worker")
logger.setLevel(logging.DEBUG)


def namespacedTopic(topic, namespace=None):
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class RabbitMQSource(SourceTap):
    """RabbitMQSource reads from RabbitMQ

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

    kind = "RABBITMQ"

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
        self.logger.info("RabbitMQSource initialized.")

    def __repr__(self):
        return 'RabbitMQSource(queue="{}")'.format(
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--rabbitmq",
            type=str,
            default=os.environ.get("RABBITMQ", "localhost"),
            help="RabbitMQ host",
        )

    def read(self):
        timedOut = False
        lastMessageTime = time.time()

        while not timedOut:
            try:
                method, header, body = self.channel.basic_get(self.name)
            except pika.exceptions.AMQPConnectionError:
                self.logger.warning("Trying to restore connection to RabbitMQ...")
                parameters = pika.ConnectionParameters(self.config.rabbitmq)
                self.rabbit = pika.BlockingConnection(parameters)
                self.channel = self.rabbit.channel()
                self.logger.warning("Connection to RabbitMQ restored.")
                method, header, body = self.channel.basic_get(self.name)
            except Exception as ex:
                self.logger.error(ex)
                break

            if method:
                self.delivery_tag = method.delivery_tag
                self.logger.info("Read message %s", self.delivery_tag)
                yield self.messageClass.deserialize(body, config=self.config)
                lastMessageTime = time.time()
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                self.logger.info("RabbitMQSource timed out.")
                timedOut = True

    def acknowledge(self):
        self.logger.info("acknowledged message %s", self.delivery_tag)
        self.channel.basic_ack(self.delivery_tag)

    def close(self):
        self.logger.info("RabbitMQSource closed.")
        self.channel.close()
        self.rabbit.close()


class RabbitMQDestination(DestinationTap):
    """RabbitMQDestination writes to RabbitMQ

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

    kind = "RABBITMQ"

    def __init__(self, config, logger=logger):
        super().__init__(config, logger)
        self.config = config
        self.topic = config.out_topic
        self.name = namespacedTopic(config.out_topic, config.namespace)
        parameters = pika.ConnectionParameters(config.rabbitmq, heartbeat=5)
        self.rabbit = pika.BlockingConnection(parameters)
        self.channel = self.rabbit.channel()
        self.channel.queue_declare(queue=self.name)
        self.logger.info("RabbitMQDestination initialized.")

    def __repr__(self):
        return 'RabbitMQDestination(queue="{}")'.format(
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--rabbitmq",
            type=str,
            default=os.environ.get("RABBITMQ", "localhost"),
            help="RabbitMQ host",
        )

    def write(self, message):
        try:
            serialized = message.serialize()
            self.channel.basic_publish(
                exchange="", routing_key=self.name, body=serialized
            )
            return len(serialized)
        except pika.exceptions.StreamLostError:
            self.logger.warning("Trying to restore connection to RabbitMQ...")
            self.rabbit = pika.BlockingConnection(
                pika.ConnectionParameters(self.config.rabbitmq)
            )
            self.channel = self.rabbit.channel()
            self.logger.warning("Connection to RabbitMQ restored.")
            self.channel.basic_publish(
                exchange="", routing_key=self.name, body=message.serialize()
            )

    def close(self):
        self.logger.info("RabbitMQDestination closed.")
        self.channel.close()
        self.rabbit.close()
