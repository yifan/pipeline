import time
import uuid
import logging
from logging import Logger
from typing import Iterator, ClassVar, Any

from pydantic import Field
from redis import Redis
from rq import Queue

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase, Message
from ..helpers import namespaced_topic


pipelineLogger = logging.getLogger("pipeline")


class RQSourceSettings(SourceSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")
    min_idle_time: int = Field(
        3600000,
        title="messages not acknowledged after min-idle-time will be reprocessed",
    )


class RQSource(SourceTap):
    """RQSource reads from Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RQSourceSettings()
    >>> RQSource(settings=settings, logger=logging)
    RQSource(host="redis://localhost:6379/0", topic="in-topic")
    """

    settings: RQSourceSettings
    redis: Any

    kind: ClassVar[str] = "RQ"

    def __init__(
        self, settings: RQSourceSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.timeout = settings.timeout
        self.redis = Redis.from_url(settings.redis)
        self.queue = Queue(self.topic, connection=self.redis)
        self.consumer = str(uuid.uuid1())
        self.last_msg = None
        self.last_job = None

    def __repr__(self) -> str:
        return f'RQSource(host="{self.settings.redis}", topic="{self.topic}")'

    def __len__(self) -> int:
        return len(self.queue)

    def read(self) -> Iterator[MessageBase]:
        timedOut = False
        last_message_time = time.time()

        while not timedOut:
            if self.queue.is_empty():
                time.sleep(0.01)
                continue

            try:
                jobs = self.queue.get_jobs(length=1)
                if len(jobs) > 0:
                    job = jobs[0]

                    self.logger.info("Read message %s", job.id)
                    data = job.args[0]

                    self.last_job = job
                    yield Message(content=data)
                    last_message_time = time.time()
                else:
                    time.sleep(0.01)

            except Exception as ex:
                self.logger.error(ex)
                break

            if self.timeout > 0:
                elapsed_time = time.time() - last_message_time
                if elapsed_time > self.timeout:
                    self.logger.warning(
                        "reader timed out after %d seconds (timeout %d)",
                        elapsed_time,
                        self.timeout,
                    )
                    timedOut = True

    def acknowledge(self) -> None:
        if self.last_job:
            self.last_job.delete()
            self.last_job = None

    def close(self) -> None:
        self.redis.close()


class RQDestinationSettings(DestinationSettings):
    redis: str = Field("redis://localhost:6379/0", title="redis url")
    task: str = Field("base.tasks.do_task", title="task name for rq")


class RQDestination(DestinationTap):
    """RQDestination writes to Redis Stream

    >>> import logging
    >>> from unittest.mock import patch
    >>> settings = RQDestinationSettings()
    >>> RQDestination(settings=settings, logger=logging)
    RQDestination(host="redis://localhost:6379/0", topic="out-topic")
    """

    settings: RQDestinationSettings
    redis: Any

    kind: ClassVar[str] = "RQ"

    def __init__(
        self, settings: RQDestinationSettings, logger: Logger = pipelineLogger
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.topic = namespaced_topic(settings.topic, settings.namespace)
        self.redis = Redis.from_url(settings.redis)
        self.queue = Queue(self.topic, connection=self.redis)

    def __repr__(self) -> str:
        return f'RQDestination(host="{self.settings.redis}", topic="{self.topic}")'

    def __len__(self) -> int:
        return len(self.queue)

    def write(self, message: MessageBase) -> int:
        def _safe_id(obj):
            return str(obj.get("id")) if "id" in obj else None

        self.queue.enqueue(
            self.settings.task,
            message.content,
            result_ttl=0,
            description=_safe_id(message.content) or message.id,
        )
        return 0

    def close(self) -> None:
        self.redis.close()
