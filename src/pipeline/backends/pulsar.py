import os
import time

import pulsar

from ..tap import SourceTap, DestinationTap


class PulsarSource(SourceTap):
    """PulsarSource reads from Pulsar

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> PulsarSource.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pulsar.Client') as c:
    ...     PulsarSource(config, logger=logging)
    PulsarSource(host="pulsar://pulsar.pulsar.svc.cluster.local:6650",name="persistent://meganews/test/in-topic",subscription="subscription")
    """

    kind = "PULSAR"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = pulsar.Client(config.pulsar)
        self.tenant = config.tenant
        self.namespace = config.namespace
        self.topic = config.in_topic
        self.subscription = config.subscription
        self.name = "persistent://{}/{}/{}".format(
            self.tenant, self.namespace, self.topic
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
        parser.add_argument(
            "--pulsar",
            type=str,
            default=os.environ.get(
                "PULSAR", "pulsar://pulsar.pulsar.svc.cluster.local:6650"
            ),
            help="pulsar address",
        )
        parser.add_argument(
            "--tenant",
            type=str,
            default=os.environ.get("TENANT", "meganews"),
            help="pulsar tenant, always is meganews",
        )
        parser.add_argument(
            "--namespace",
            type=str,
            default=os.environ.get("NAMESPACE", "test"),
            help="pulsar namespace (default: test",
        )
        parser.add_argument(
            "--subscription",
            type=str,
            default=os.environ.get("SUBSCRIPTION", "subscription"),
            help="subscription to read",
        )

    def read(self):
        timedOut = False
        lastMessageTime = time.time()
        while not timedOut:
            try:
                msg = self.consumer.receive()
                self.last_msg = msg
                yield self.messageClass.deserialize(msg.data(), config=self.config)
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
    """PulsarDestination writes to Pulsar

    >>> import logging
    >>> from unittest.mock import patch
    >>> from argparse import ArgumentParser
    >>> parser = ArgumentParser(conflict_handler='resolve')
    >>> PulsarDestination.add_arguments(parser)
    >>> config = parser.parse_args([])
    >>> with patch('pulsar.Client') as c:
    ...     PulsarDestination(config, logger=logging)
    PulsarDestination(host="pulsar://pulsar.pulsar.svc.cluster.local:6650",name="persistent://meganews/test/out-topic")
    """

    kind = "PULSAR"

    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.config = config
        self.client = pulsar.Client(config.pulsar)
        self.tenant = config.tenant
        self.namespace = config.namespace
        self.topic = config.out_topic
        self.name = "persistent://{}/{}/{}".format(
            self.tenant, self.namespace, self.topic
        )
        self.producer = self.client.create_producer(self.name)

    def __repr__(self):
        return 'PulsarDestination(host="{}",name="{}")'.format(
            self.config.pulsar,
            self.name,
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "--pulsar",
            type=str,
            default=os.environ.get(
                "PULSAR", "pulsar://pulsar.pulsar.svc.cluster.local:6650"
            ),
            help="pulsar address",
        )
        parser.add_argument(
            "--tenant",
            type=str,
            default=os.environ.get("TENANT", "meganews"),
            help="pulsar tenant, always is meganews",
        )
        parser.add_argument(
            "--namespace",
            type=str,
            default=os.environ.get("NAMESPACE", "test"),
            help="pulsar namespace (default: test)",
        )

    def write(self, message):
        serialized = message.serialize()
        self.producer.send(serialized)
        return len(serialized)

    def close(self):
        self.client.close()
