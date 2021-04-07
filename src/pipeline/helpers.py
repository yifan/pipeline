import time
from argparse import ArgumentParser, Action

from typing import List
from pydantic import BaseSettings


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
        settingsRef: Settings = self

        class SetSettingsAction(Action):
            settings = settingsRef

            def __call__(self, parser, namespace, values, option_string=None):
                if settingsRef.Config.env_prefix:
                    dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    dest = self.dest
                if hasattr(settingsRef, dest):
                    setattr(settingsRef, dest, values)

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
    def __init__(self):
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()
        self.totalTime = 0.0
        self.totalProcessTime = 0.0
        self.timeCount = 0
        self.processTimeCount = 0

    def elapsed_time(self):
        t = time.perf_counter() - self.startTime
        self.totalTime += t
        self.timeCount += 1
        return t

    def average_time(self):
        return self.totalTime / self.timeCount

    def process_time(self):
        t = time.process_time() - self.startProcessTime
        self.totalProcessTime += t
        self.processTimeCount += 1
        return t

    def average_process_time(self):
        return self.totalProcessTime / self.processTimeCount

    def start(self):
        self.startTime = time.perf_counter()
        self.startProcessTime = time.process_time()

    def log(self, logger):
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
