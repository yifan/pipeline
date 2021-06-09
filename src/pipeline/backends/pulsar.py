import time
from logging import Logger
from typing import ClassVar, Iterator

import pulsar
from pydantic import AnyUrl, Field

from ..tap import SourceTap, DestinationTap, SourceSettings, DestinationSettings
from ..message import MessageBase


class PulsarDsn(AnyUrl):
    allowed_schemes = {
        "pulsar",
    }


class PulsarSourceSettings(SourceSettings):
    pulsar: PulsarDsn = Field("pulsar://localhost:6650", title="pulsar url")
    tenant: str = Field(None, title="pulsar tenant, always is meganews")
    subscription: str = Field(None, title="subscription to read")


class PulsarSource(SourceTap):
    """PulsarSource reads from Pulsar

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = PulsarSourceSettings(namespace='test', tenant='tenant', subscription='subscription')
    >>> with patch('pulsar.Client') as c:
    ...     PulsarSource(settings=settings, logger=logging)
    PulsarSource(host="pulsar://localhost:6650",name="persistent://tenant/test/in-topic",subscription="subscription")
    """

    settings: PulsarSourceSettings

    kind: ClassVar[str] = "PULSAR"

    def __init__(self, settings: PulsarSourceSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = pulsar.Client(settings.pulsar)
        self.tenant = settings.tenant
        self.namespace = settings.namespace
        self.topic = settings.topic
        self.subscription = settings.subscription
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

    def __repr__(self) -> str:
        return 'PulsarSource(host="{}",name="{}",subscription="{}")'.format(
            self.settings.pulsar,
            self.name,
            self.subscription,
        )

    def read(self) -> Iterator[MessageBase]:
        timeout_ms = self.settings.timeout * 1000 if self.settings.timeout else None
        msg = self.consumer.receive(timeout_millis=timeout_ms)
        while msg:
            msg = self.consumer.receive(timeout_millis=timeout_ms)
            self.last_msg = msg
            if msg:
                yield MessageBase.deserialize(msg.data())
            time.sleep(0.01)

    def acknowledge(self) -> None:
        self.consumer.acknowledge(self.last_msg)

    def close(self) -> None:
        self.client.close()


class PulsarDestinationSettings(DestinationSettings):
    pulsar: PulsarDsn = Field("pulsar://localhost:6650", title="pulsar url")
    tenant: str = Field(None, title="pulsar tenant, always is meganews")


class PulsarDestination(DestinationTap):
    """PulsarDestination writes to Pulsar

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = PulsarDestinationSettings(namespace='test', tenant='tenant')
    >>> with patch('pulsar.Client') as c:
    ...     PulsarDestination(settings=settings, logger=logging)
    PulsarDestination(host="pulsar://localhost:6650",name="persistent://tenant/test/out-topic")
    """

    settings: PulsarDestinationSettings

    kind: ClassVar[str] = "PULSAR"

    def __init__(self, settings: PulsarDestinationSettings, logger: Logger) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.client = pulsar.Client(settings.pulsar)
        self.tenant = settings.tenant
        self.namespace = settings.namespace
        self.topic = settings.topic
        self.name = "persistent://{}/{}/{}".format(
            self.tenant, self.namespace, self.topic
        )
        self.producer = self.client.create_producer(self.name)

    def __repr__(self) -> str:
        return 'PulsarDestination(host="{}",name="{}")'.format(
            self.settings.pulsar,
            self.name,
        )

    def write(self, message: MessageBase) -> int:
        serialized = message.serialize(compress=self.settings.compress)
        self.producer.send(serialized)
        return len(serialized)

    def close(self) -> None:
        self.client.close()
