import time
import json
from logging import Logger
from typing import ClassVar, Iterator, Optional

from pydantic import Field
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase


class KafkaSourceSettings(SourceSettings):
    kafka: str = Field("localhost", title="kafka url")
    group_id: str = Field(None, title="kafka consumer group id")
    config: Optional[str] = Field(None, title="kafka config in json format")
    poll_timeout: int = Field(30, title="time out for polling new messages")


class KafkaSource(SourceTap):
    """KafkaSource reads from KAFKA

    >>> import logging
    >>> settings = KafkaSourceSettings(group_id="group-id", config='{"sasl.mechanisms": "PLAIN"}')
    >>> KafkaSource(settings=settings, logger=logging)
    KafkaSource(host="localhost",groupid="group-id",topic="in-topic")
    """

    settings: KafkaSourceSettings

    kind: ClassVar[str] = "KAFKA"

    def __init__(self, settings: KafkaSourceSettings, logger: Logger) -> None:
        super().__init__(settings, logger)

        config = {
            "bootstrap.servers": settings.kafka,
            "broker.address.family": "v4",
            "group.id": settings.group_id,
            "auto.offset.reset": "earliest",
            # 'log_level': 0,
            # 'debug': 'consumer',
            "enable.auto.commit": "false",
        }

        if settings.config:
            user_config = json.loads(settings.config)
            config.update(user_config)

        self.consumer = Consumer(config, logger=self.logger)
        self.topic = settings.topic
        self.last_msg = None

        def maybe_rewind(c, partitions):  # type: ignore
            self.logger.info("Assignment: %s", str(partitions))
            # disable rewind for now
            # if settings.rewind:
            #     for partition in partitions:
            #         partition.offset = OFFSET_BEGINNING
            #     self.logger.info("Rewind, new assignment: %s", str(partitions))
            #     c.assign(partitions)

        self.consumer.subscribe([self.topic], on_assign=maybe_rewind)
        self.logger.info("KAFKA consumer subscribed to topic %s", self.topic)

    def __repr__(self) -> str:
        return 'KafkaSource(host="{}",groupid="{}",topic="{}")'.format(
            self.settings.kafka, self.settings.group_id, self.topic
        )

    def read(self) -> Iterator[MessageBase]:
        # sometimes it is better to make constant call to KAFKA
        # to keep the connection alive.
        timedout = False
        last_message_time = time.time()
        while not timedout:
            msg = self.consumer.poll(timeout=self.settings.poll_timeout)

            if msg:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.warning("Reaching EOF")
                        break
                    else:
                        raise KafkaException(msg.error())
                else:
                    self.logger.info("Read {}, {}".format(msg.topic(), msg.offset()))
                    self.last_msg = msg
                    yield MessageBase.deserialize(msg.value())
                    last_message_time = time.time()

            if self.settings.timeout > 0:
                time_since_last_message = time.time() - last_message_time
                timedout = time_since_last_message > self.settings.timeout

    def acknowledge(self) -> None:
        if self.last_msg:
            self.consumer.commit(message=self.last_msg)

    def close(self) -> None:
        self.consumer.close()


class KafkaDestinationSettings(DestinationSettings):
    kafka: str = Field("localhost", title="kafka url")
    config: str = Field("{}", title="kafka config in json format")


class KafkaDestination(DestinationTap):
    """KafkaDestination writes to KAFKA

    >>> import logging
    >>> settings = KafkaDestinationSettings()
    >>> KafkaDestination(settings=settings, logger=logging)
    KafkaDestination(host="localhost",topic="out-topic")
    """

    settings: KafkaDestinationSettings

    kind: ClassVar[str] = "KAFKA"

    def __init__(self, settings: KafkaDestinationSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        config = {
            "bootstrap.servers": settings.kafka,
            "queue.buffering.max.ms": 100,
            "message.send.max.retries": 5,
            "request.required.acks": "all",
            "broker.address.family": "v4",
        }

        if settings.config:
            user_config = json.loads(settings.config)
            config.update(user_config)

        self.topic = settings.topic
        self.producer = Producer(config, logger=self.logger)

    def __repr__(self) -> str:
        return 'KafkaDestination(host="{}",topic="{}")'.format(
            self.settings.kafka, self.topic
        )

    def write(self, message: MessageBase) -> int:
        def delivery_report(err, msg):  # type: ignore
            if err:
                self.logger.error(
                    "Message delivery failed ({} [{}]: {}".format(
                        msg.topic(), str(msg.partition()), err
                    )
                )
            else:
                self.logger.info(
                    "Message delivered to {} [{}] {}".format(
                        msg.topic(), msg.partition(), msg.offset()
                    )
                )

        serialized = message.serialize(compress=self.settings.compress)
        self.producer.produce(self.topic, serialized, callback=delivery_report)
        self.producer.flush()
        return len(serialized)

    def close(self) -> None:
        self.producer.flush()
