import time
from argparse import ArgumentParser, Action
from typing import List
from logging import Logger

from pydantic import BaseSettings


def namespaced_topic(topic: str, namespace: str = None) -> str:
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


class Settings(BaseSettings):
    """Settings can read from environment variable and parse
    command line arguments as well.

    Usage:
    >>> class ASettings(Settings):
    ...     name:str
    ...     class Config:
    ...         env_prefix = 'in_'
    >>> settings = ASettings(name='name')
    >>> settings.name
    'name'
    >>> settings.parse_args("--in-name yifan".split())
    >>> settings.name
    'yifan'
    """

    def parse_args(self, args: List[str]) -> None:
        settings_ref: Settings = self

        class SetSettingsAction(Action):
            settings = settings_ref

            def __call__(self, parser, namespace, values, option_string=None):  # type: ignore
                if self.settings.Config.env_prefix:
                    dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    dest = self.dest
                if hasattr(self.settings, dest):
                    setattr(self.settings, dest, values)

        parser = ArgumentParser(add_help=True)
        for name, field in self.__fields__.items():
            name = (self.Config.env_prefix + name).replace("_", "-")
            parser.add_argument(
                f"--{name}",
                type=field.type_,
                action=SetSettingsAction,
                help=field.field_info.title,
            )
        parser.parse_known_args(args)


class Timer:
    def __init__(self) -> None:
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()
        self.totalTime = 0.0
        self.totalProcessTime = 0.0
        self.timeCount = 0
        self.processTimeCount = 0

    def elapsed_time(self) -> float:
        t = time.perf_counter() - self.startTime
        self.totalTime += t
        self.timeCount += 1
        return t

    def average_time(self) -> float:
        return self.totalTime / self.timeCount

    def process_time(self) -> float:
        t = time.process_time() - self.startProcessTime
        self.totalProcessTime += t
        self.processTimeCount += 1
        return t

    def average_process_time(self) -> float:
        return self.totalProcessTime / self.processTimeCount

    def start(self) -> None:
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()

    def log(self, logger: Logger) -> None:
        elapsedTime = self.elapsed_time()
        averageTime = self.average_time()
        logger.info("Elapsed Time: %.2f, Average Time: %.2f", elapsedTime, averageTime)
        processTime = self.process_time()
        averageProcessTime = self.average_process_time()
        logger.info(
            "Process Time: %.2f, Average Process Time: %.2f",
            processTime,
            averageProcessTime,
        )
