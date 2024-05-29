import logging
from logging import Logger
import json
import asyncio
from collections import deque
from typing import Iterator, ClassVar

from aiohttp import web

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase, Message


pipelineLogger = logging.getLogger("pipeline")


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Server(metaclass=Singleton):
    def __init__(self, host, port, path="/", logger=pipelineLogger) -> None:
        self.host = host
        self.port = port
        self.path = path
        self.logger = logger
        self._request = None
        self._result = None
        self.started = False
        self.request_condition = asyncio.Condition()
        self.result_condition = asyncio.Condition()

    def start(self, loop):
        if not self.started:
            loop.set_debug(True)
            loop.run_until_complete(self.start_server())
            self.started = True

    async def start_async(self):
        if not self.started:
            await self.start_server()
            self.started = True

    async def start_server(self):
        self.app = web.Application()
        self.app.router.add_post(self.path, self.handle_request)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.server = web.TCPSite(self.runner, self.host, self.port)
        await self.server.start()
        self.logger.info(f"HTTP server started at {self.host}:{self.port}")  # NOQA

    async def handle_request(self, request):
        try:
            # only handle one request at a time
            # self.logger.info("Received HTTP request")
            data = await request.json()
            self._request = data

            # notify to process
            async with self.request_condition:
                self.request_condition.notify()

            # wait for result
            async with self.result_condition:
                await self.result_condition.wait()

            if self._result is None:
                return web.Response(status=500, text="something thing wrong")

            result = self._result
            self._result = None
            return web.json_response(result)

        except json.JSONDecodeError:
            return web.Response(status=400, text="failure")

    async def read_request(self):
        async with self.request_condition:
            await self.request_condition.wait()

        return self._request

    async def write_response(self, response):
        async with self.result_condition:
            self._result = response
            self.result_condition.notify()

    async def close(self):
        if self.started:
            await self.server.stop()
            self.started = False


class HTTPServerSourceSettings(SourceSettings):
    host: str = "127.0.0.1"
    port: int = 8080
    path: str = "/"


class HTTPServerSource(SourceTap):
    """listen to a http request"""

    settings: HTTPServerSourceSettings
    messages: deque
    server: Server

    kind: ClassVar[str] = "HTTPSERVER"

    def __init__(
        self,
        settings: HTTPServerSourceSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.server = Server(settings.host, settings.port, settings.path, logger)
        self.loop = asyncio.get_event_loop()

    def __repr__(self) -> str:
        return (
            f'HTTPServerSource(host="{self.settings.host}"'
            f', port="{self.settings.port}", path="{self.settings.path}")'
        )

    def __len__(self) -> int:
        return len(self.queue)

    def read(self) -> Iterator[MessageBase]:
        self.server.start(self.loop)
        while True:
            message = Message(
                id="none",
                content=self.loop.run_until_complete(self.server.read_request()),
            )
            if message is not None:
                yield message

    def acknowledge(self) -> None:
        pass

    def close(self) -> None:
        self.server.close()


class HTTPServerDestinationSettings(DestinationSettings):
    host: str = "127.0.0.1"
    port: int = 8080
    path: str = "/"


class HTTPServerDestination(DestinationTap):

    settings: HTTPServerDestinationSettings
    server: Server

    kind: ClassVar[str] = "HTTPSERVER"

    def __init__(
        self,
        settings: HTTPServerDestinationSettings,
        logger: Logger = pipelineLogger,
    ) -> None:
        super().__init__(settings, logger)
        self.settings = settings
        self.server = Server(settings.host, settings.port, settings.path)
        self.loop = asyncio.get_event_loop()

    def __repr__(self) -> str:
        return (
            f'HTTPServerDestination(host="{self.settings.host}", '
            f'port="{self.settings.port}", path="{self.settings.path}")'
        )

    def __len__(self) -> int:
        return 0

    def write(self, message: MessageBase) -> int:
        self.loop.run_until_complete(self.server.write_response(message.content))
        return 0

    def close(self) -> None:
        self.server.close()
