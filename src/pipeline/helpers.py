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
    >>> parser = settings.parse_args("--in-name yifan".split())
    >>> settings.name
    'yifan'
    """

    def parse_args(self, args: List[str], parser=None) -> ArgumentParser:
        settings_ref: Settings = self

        class BooleanOptionalAction(Action):
            settings = settings_ref

            def __init__(  # type: ignore
                self,
                option_strings,
                dest,
                default=None,
                type=None,
                choices=None,
                required=False,
                help=None,
                metavar=None,
            ):

                _option_strings = []
                for option_string in option_strings:
                    _option_strings.append(option_string)

                    if option_string.startswith("--"):
                        option_string = "--no-" + option_string[2:]
                        _option_strings.append(option_string)

                if help is not None and default is not None:
                    help += f" (default: {default})"

                super().__init__(
                    option_strings=_option_strings,
                    dest=dest,
                    nargs=0,
                    default=default,
                    type=type,
                    choices=choices,
                    required=required,
                    help=help,
                    metavar=metavar,
                )

            def __call__(self, parser, namespace, values, option_string=None):  # type: ignore
                if self.settings.Config.env_prefix:
                    dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    dest = self.dest
                if option_string in self.option_strings:
                    setattr(self.settings, dest, not option_string.startswith("--no-"))

            def format_usage(self):  # type: ignore
                return " | ".join(self.option_strings)

        class SetSettingsAction(Action):
            settings = settings_ref

            def __call__(self, parser, namespace, values, option_string=None):  # type: ignore
                if self.settings.Config.env_prefix:
                    dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    dest = self.dest
                if hasattr(self.settings, dest):
                    setattr(self.settings, dest, values)

        if parser is None:
            parser = ArgumentParser(add_help=False)

        for name, field in self.__fields__.items():
            name = (self.Config.env_prefix + name).replace("_", "-")
            if field.type_ == bool:
                action = BooleanOptionalAction
            else:
                action = SetSettingsAction
            parser.add_argument(
                f"--{name}",
                type=field.type_,
                action=action,
                help=field.field_info.title,
            )
        options = parser.parse_known_args(args)
        return options


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
