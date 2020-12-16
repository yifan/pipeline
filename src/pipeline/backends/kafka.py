import json
import os
import time

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)

from ..tap import SourceTap, DestinationTap


class KafkaSource(SourceTap):
    """KafkaSource reads from KAFKA

    >>> import logging
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> KafkaSource.add_arguments(parser)
    >>> config = parser.parse_args(["--config", '{"sasl.mechanisms": "PLAIN"}'])
    >>> KafkaSource(config, logger=logging)
    KafkaSource(host="kafka.kafka.svc.cluster.local",groupid="group-id",topic="in-topic")
    """

    kind = "KAFKA"

    def __init__(self, config, logger):
        super().__init__(config, logger)

        kafkaConfig = {
            "bootstrap.servers": config.kafka,
            "broker.address.family": "v4",
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            # 'log_level': 0,
            # 'debug': 'consumer',
            "enable.auto.commit": "false",
        }

        if config.config:
            extraConfig = json.loads(config.config)
            kafkaConfig.update(extraConfig)

        self.consumer = Consumer(kafkaConfig, logger=self.logger)
        self.topic = config.in_topic
        self.last_msg = None

        def maybe_rewind(c, partitions):
            self.logger.info("Assignment: %s", str(partitions))
            if config.rewind:
                for partition in partitions:
                    partition.offset = OFFSET_BEGINNING
                self.logger.info("Rewind, new assignment: %s", str(partitions))
                c.assign(partitions)

        self.consumer.subscribe([self.topic], on_assign=maybe_rewind)
        self.logger.info("KAFKA consumer subscribed to topic %s", self.topic)

    def __repr__(self):
        return 'KafkaSource(host="{}",groupid="{}",topic="{}")'.format(
            self.config.kafka, self.config.group_id, self.topic
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--kafka",
            type=str,
            default=os.environ.get("KAFKA", "kafka.kafka.svc.cluster.local"),
            help="kafka address",
        )
        parser.add_argument(
            "--group-id",
            type=str,
            default=os.environ.get("GROUPID", "group-id"),
            help="group id",
        )
        parser.add_argument(
            "--config",
            type=str,
            default=os.environ.get("KAFKACONFIG", None),
            help="kafka configuration in JSON format",
        )
        parser.add_argument(
            "--poll-timeout",
            type=int,
            default=30,
            help="poll new message timeout in seconds",
        )

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            msg = self.consumer.poll(timeout=self.config.poll_timeout)
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
                yield self.messageClass.deserialize(msg.value(), config=self.config)
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
    """KafkaDestination writes to KAFKA

    >>> import logging
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> KafkaDestination.add_arguments(parser)
    >>> config = parser.parse_args(["--config", '{"sasl.mechanisms": "PLAIN"}'])
    >>> KafkaDestination(config, logger=logging)
    KafkaDestination(host="kafka.kafka.svc.cluster.local",topic="out-topic")
    """

    kind = "KAFKA"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        kafkaConfig = {
            "bootstrap.servers": config.kafka,
            "queue.buffering.max.ms": 100,
            "message.send.max.retries": 5,
            "request.required.acks": "all",
            "broker.address.family": "v4",
        }

        if config.config:
            extraConfig = json.loads(config.config)
            kafkaConfig.update(extraConfig)

        self.topic = config.out_topic
        self.producer = Producer(kafkaConfig, logger=self.logger)

    def __repr__(self):
        return 'KafkaDestination(host="{}",topic="{}")'.format(
            self.config.kafka, self.topic
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--kafka",
            type=str,
            default=os.environ.get("KAFKA", "kafka.kafka.svc.cluster.local"),
            help="kafka address",
        )
        parser.add_argument(
            "--config",
            type=str,
            default=os.environ.get("KAFKACONFIG", None),
            help="kafka configuration in JSON format",
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

        serialized = message.serialize()
        self.producer.produce(self.topic, serialized, callback=delivery_report)
        self.producer.flush()
        return len(serialized)

    def close(self):
        self.producer.flush()
