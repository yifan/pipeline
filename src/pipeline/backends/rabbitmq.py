import os
import time

import pika
from pydantic import AnyUrl, Field

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import Message


def namespacedTopic(topic, namespace=None):
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class RabbitMQDsn(AnyUrl):
    allowed_schemes = {
        "amqp",
    }


class RabbitMQSourceSettings(SourceSettings):
    rabbitmq: RabbitMQDsn = Field("amqp://localhost", title="RabbitMQ host")


class RabbitMQSource(SourceTap):
    """RabbitMQSource reads from RabbitMQ

    RabbitMQ options:
        --rabbitmq (env: RABBITMQ): RabbitMQ host

    Source options:
        --in-topic (env: INTOPIC): queue to read from
        --timeout (env: TIMEOUT): seconds to exit if no new messages

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RabbitMQSourceSettings()
    >>> with patch('pika.ConnectionParameters') as c1:
    ...     with patch('pika.BlockingConnection') as c2:
    ...         RabbitMQSource(settings=settings, logger=logging)
    RabbitMQSource(queue="in-topic")
    """

    kind = "RABBITMQ"

    def __init__(self, settings, logger):
        super().__init__(settings, logger)
        self.settings = settings
        self.topic = settings.topic
        self.name = namespacedTopic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        parameters = pika.ConnectionParameters(settings.rabbitmq)
        self.rabbit = pika.BlockingConnection(parameters)
        self.channel = self.rabbit.channel()
        self.channel.queue_declare(queue=self.name)
        self.delivery_tag = None
        self.msg = None
        self.logger.info("RabbitMQSource initialized.")

    def __repr__(self):
        return f'RabbitMQSource(queue="{self.name}")'

    def read(self):
        timedOut = False
        lastMessageTime = time.time()

        while not timedOut:
            try:
                method, header, body = self.channel.basic_get(self.name)
            except pika.exceptions.AMQPConnectionError:
                self.logger.warning("Trying to restore connection to RabbitMQ...")
                parameters = pika.ConnectionParameters(self.settings.rabbitmq)
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
                yield Message.deserialize(body)
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


class RabbitMQDestinationSettings(DestinationSettings):
    rabbitmq: RabbitMQDsn = Field("amqp://localhost", title="RabbitMQ host")


class RabbitMQDestination(DestinationTap):
    """RabbitMQDestination writes to RabbitMQ

    options:
        --rabbitmq (env: RABBITMQ): RabbitMQ host

    standard options:
        --out-topic (env: OUTTOPIC): queue to write to

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RabbitMQDestinationSettings()
    >>> with patch('pika.ConnectionParameters') as c1:
    ...     with patch('pika.BlockingConnection') as c2:
    ...        RabbitMQDestination(settings=settings, logger=logging)
    RabbitMQDestination(queue="out-topic")
    """

    kind = "RABBITMQ"

    def __init__(self, settings, logger):
        super().__init__(settings, logger)
        self.settings = settings
        self.topic = settings.topic
        self.name = namespacedTopic(settings.topic, settings.namespace)
        parameters = pika.ConnectionParameters(settings.rabbitmq, heartbeat=5)
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
                pika.ConnectionParameters(self.settings.rabbitmq)
            )
            self.channel = self.rabbit.channel()
            self.logger.warning("Connection to RabbitMQ restored.")
            self.channel.basic_publish(
                exchange="",
                routing_key=self.name,
                body=message.serialize(compress=self.settings.compress),
            )

    def close(self):
        self.logger.info("RabbitMQDestination closed.")
        self.channel.close()
        self.rabbit.close()
