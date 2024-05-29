import logging
from logging import Logger
import json
import asyncio
from collections import deque
from typing import Iterator, ClassVar

from aiohttp import web
import requests

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase


pipelineLogger = logging.getLogger("pipeline")


class HTTPSourceSettings(SourceSettings):
    host: str = "127.0.0.1"
    port: int = 8080
    path: str = "/"


class HTTPSource(SourceTap):
    """listen to a http request"""

    settings: HTTPSourceSettings
    messages: deque
    server: web.TCPSite

    kind: ClassVar[str] = "HTTP"

    def __init__(
        self,
        settings: HTTPSourceSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger=pipelineLogger)
        self.settings = settings
        self.logger = logger
        self.messages = deque()

        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(True)
        self.loop.run_until_complete(self.start_server())

    async def start_server(self):
        self.app = web.Application()
        self.app.router.add_post(self.settings.path, self.handle_request)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.server = web.TCPSite(self.runner, self.settings.host, self.settings.port)
        await self.server.start()
        self.logger.info(
            f"HTTP server started at {self.settings.host}:{self.settings.port}"  # NOQA
        )

    async def handle_request(self, request):
        try:
            self.logger.info("Received HTTP request")
            data = await request.read()
            self.messages.append(MessageBase.deserialize(data))
            return web.Response(text="success")
        except json.JSONDecodeError:
            return web.Response(status=400, text="failure")

    def __repr__(self) -> str:
        return f'HTTPSource(host="{self.settings.host}", port="{self.settings.port}", path="{self.settings.path}")'

    def __len__(self) -> int:
        return len(self.queue)

    def read(self) -> Iterator[MessageBase]:
        while True:
            if not self.messages:
                task = self.loop.create_task(asyncio.sleep(0.01))
                self.loop.run_until_complete(task)
                continue
            message = self.messages.popleft()
            if message is not None:
                print("read message in read()")
                yield message

    def acknowledge(self) -> None:
        pass

    def close(self) -> None:
        self.loop.run_until_complete(self.server.stop())
        self.loop.run_until_complete(self.runner.cleanup())


class HTTPDestinationSettings(DestinationSettings):
    host: str = "127.0.0.1"
    port: int = 8080
    path: str = "/"


class HTTPDestination(DestinationTap):

    settings: HTTPDestinationSettings

    kind: ClassVar[str] = "HTTP"

    def __init__(
        self,
        settings: HTTPDestinationSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger=pipelineLogger)
        self.settings = settings
        self.logger = logger

    def __repr__(self) -> str:
        return f'HTTPDestination(host="{self.settings.host}", port="{self.settings.port}", path="{self.settings.path}")'

    def __len__(self) -> int:
        return 0

    def write(self, message: MessageBase) -> int:
        headers = {"Content-Type": "application/octet-stream"}
        response = requests.post(
            f"http://{self.settings.host}:{self.settings.port}{self.settings.path}",  # NOQA
            headers=headers,
            data=message.serialize(),
        )
        response.raise_for_status()
        return 0

    def close(self) -> None:
        pass
