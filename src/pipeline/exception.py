from typing import Any
from logging import Logger


class PipelineError(Exception):
    def __init__(self, message: str, data: Any = None, traceback: str = None) -> None:
        super().__init__(message)
        self.data = data
        self.traceback = traceback

    def log(self, logger: Logger) -> None:
        if self.data:
            if hasattr(self.data, "log"):
                self.data(logger)
            else:
                logger.warning(self.data)
        if self.traceback:
            logger.warning(self.traceback)


class PipelineInputError(PipelineError):
    pass


class PipelineOutputError(PipelineError):
    pass


class PipelineMessageError(PipelineError):
    pass
