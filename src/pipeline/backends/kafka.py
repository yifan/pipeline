import time

from pydantic import Json, Field
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import Message


class KafkaSourceSettings(SourceSettings):
    kafka: str = Field("localhost", title="kafka url")
    group_id: str = Field(None, title="kafka consumer group id")
    config: Json = Field(None, title="kafka config in json format")
    poll_timeout: int = Field(
        30, title="time out for polling new messages"
    )  # TODO check if duplicate with timeout


class KafkaSource(SourceTap):
    """KafkaSource reads from KAFKA

    >>> import logging
    >>> settings = KafkaSourceSettings(group_id="group-id", config='{"sasl.mechanisms": "PLAIN"}')
    >>> KafkaSource(settings=settings, logger=logging)
    KafkaSource(host="localhost",groupid="group-id",topic="in-topic")
    """

    kind = "KAFKA"

    def __init__(self, settings, logger):
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
            config.update(settings.config)

        self.consumer = Consumer(config, logger=self.logger)
        self.topic = settings.topic
        self.last_msg = None

        def maybe_rewind(c, partitions):
            self.logger.info("Assignment: %s", str(partitions))
            # disable rewind for now
            # if settings.rewind:
            #     for partition in partitions:
            #         partition.offset = OFFSET_BEGINNING
            #     self.logger.info("Rewind, new assignment: %s", str(partitions))
            #     c.assign(partitions)

        self.consumer.subscribe([self.topic], on_assign=maybe_rewind)
        self.logger.info("KAFKA consumer subscribed to topic %s", self.topic)

    def __repr__(self):
        return 'KafkaSource(host="{}",groupid="{}",topic="{}")'.format(
            self.settings.kafka, self.settings.group_id, self.topic
        )

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            msg = self.consumer.poll(timeout=self.settings.poll_timeout)
            if msg is None:
                self.logger.warning("No message to read, timed out")
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.warning("Reaching EOF")
                    break
                else:
                    raise KafkaException(msg.error())
            else:
                self.logger.info("Read {}, {}".format(msg.topic(), msg.offset()))
                self.last_msg = msg
                yield Message.deserialize(msg.value(), compress=self.settings.compress)
                lastMessageTime = time.time()
            time.sleep(0.01)
            if self.timeout > 0 and time.time() - lastMessageTime > self.timeout:
                timedOut = True

    def acknowledge(self):
        if self.last_msg:
            self.consumer.commit(message=self.last_msg)

    def close(self):
        self.consumer.close()


class KafkaDestinationSettings(DestinationSettings):
    kafka: str = Field("localhost", title="kafka url")
    config: Json = Field(None, title="kafka config in json format")


class KafkaDestination(DestinationTap):
    """KafkaDestination writes to KAFKA

    >>> import logging
    >>> settings = KafkaDestinationSettings()
    >>> KafkaDestination(settings=settings, logger=logging)
    KafkaDestination(host="localhost",topic="out-topic")
    """

    kind = "KAFKA"

    def __init__(self, settings, logger):
        super().__init__(settings, logger)
        config = {
            "bootstrap.servers": settings.kafka,
            "queue.buffering.max.ms": 100,
            "message.send.max.retries": 5,
            "request.required.acks": "all",
            "broker.address.family": "v4",
        }

        if settings.config:
            config.update(settings.config)

        self.topic = settings.topic
        self.producer = Producer(config, logger=self.logger)

    def __repr__(self):
        return 'KafkaDestination(host="{}",topic="{}")'.format(
            self.settings.kafka, self.topic
        )

    def write(self, message):
        def delivery_report(err, msg):
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

    def close(self):
        self.producer.flush()
