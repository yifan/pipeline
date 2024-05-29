import logging
from logging import Logger
import json
import asyncio
from collections import deque
from typing import Iterator, ClassVar

from aiohttp import web

from ..tap import SourceTap, SourceSettings, DestinationTap, DestinationSettings
from ..message import MessageBase


pipelineLogger = logging.getLogger("pipeline")


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Server(object):
    __metaclass__ = Singleton

    def __init__(self, host, port, path="/", logger=pipelineLogger) -> None:
        self.host = host
        self.port = port
        self.path = path
        self.logger = logger
        self._request = None
        self._result = None
        self.started = False

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
        print("Server: handle request")
        if self._request is not None:
            print("Server: _request is not None")
            return web.Response(status=503, text="server busy")

        try:
            # only handle one request at a time
            # self.logger.info("Received HTTP request")
            data = await request.read()
            self._request = data

            print("Server: waiting for result")
            # wait for job to be done
            while self._result is None:
                await asyncio.sleep(0.01)

            self._request = None

            if self._result is None:
                return web.Response(status=500, text="something thing wrong")

            result = self._result
            self._result = None
            return web.Response(status=200, body=result)

        except json.JSONDecodeError:
            return web.Response(status=400, text="failure")

    async def read_request(self):
        print("Server: read_request waiting")
        while self._request is None:
            await asyncio.sleep(0.01)

        print("Server: read_request read")

        return self._request

    async def write_response(self, response):
        print("Server: write_response waiting")
        while self._response is not None:
            await asyncio.sleep(0.01)

        print("Server: write_response written")
        self._response = response

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
        return f'HTTPSource(host="{self.settings.host}", port="{self.settings.port}", path="{self.settings.path}")'

    def __len__(self) -> int:
        return len(self.queue)

    def read(self) -> Iterator[MessageBase]:
        self.server.start(self.loop)
        while True:
            message = MessageBase.deserialize(
                self.loop.run_until_complete(self.server.read_request())
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

    def __repr__(self) -> str:
        return (
            f'HTTPServerDestination(host="{self.settings.host}", '
            f'port="{self.settings.port}", path="{self.settings.path}")'
        )

    def __len__(self) -> int:
        return 0

    def write(self, message: MessageBase) -> int:
        data = message.serialize()
        self.server.write_response(data)
        return len(data)

    def close(self) -> None:
        self.server.close()
