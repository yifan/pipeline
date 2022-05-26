import sys
import time
from argparse import ArgumentParser, Action
from typing import List, Dict, Any, Optional, Union
from os import PathLike
from logging import Logger

from pydantic import BaseSettings


def namespaced_topic(topic: str, namespace: str = None) -> str:
    if namespace:
        return "{}/{}".format(namespace, topic)
    else:
        return topic


def parse_mappings(mappings: str) -> Dict[str, str]:
    d = {}
    if mappings:
        for pair in mappings.split(","):
            map_from, map_to = (n.strip() for n in pair.split(":"))
            d[map_from] = map_to
    return d


StrPath = Union[str, PathLike]
env_file_sentinel = str(object())


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
    >>> options, _ = settings.parse_args(args="--in-name pipeline".split())
    >>> settings.name
    'pipeline'
    >>> settings = ASettings(_args="--in-name yifan".split())
    >>> settings.name
    'yifan'
    """

    def __init__(
        __pipeline_self,
        _env_file: Optional[StrPath] = env_file_sentinel,
        _env_file_encoding: Optional[str] = None,
        _secrets_dir: Optional[StrPath] = None,
        _args: List[str] = sys.argv,
        _parser: Optional[ArgumentParser] = None,
        **values: Any,
    ) -> None:
        init_kwargs = __pipeline_self._build_values_args(
            values,
            args=_args,
            parser=_parser,
        )
        super(Settings, __pipeline_self).__init__(
            _env_file=_env_file,
            _env_file_encoding=_env_file_encoding,
            _secrets_dir=_secrets_dir,
            **init_kwargs,
        )

    def _build_values_args(
        self,
        values: Dict[str, Any],
        args: List[str],
        parser: Optional[ArgumentParser] = None,
    ) -> Dict[str, Any]:
        options, unknown = self.parse_args(args, parser=parser, update=False)
        args = vars(options)

        for name, field in self.__fields__.items():
            fullname = self.Config.env_prefix + name
            value = args.get(fullname, None)
            if value is not None:
                values[name] = value

        return values

    def parse_args(self, args: List[str], parser=None, update=True) -> ArgumentParser:
        """
        :param args:
        :param parser:
        :param update: True if update Settings in place
        """
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
                dest = self.dest
                if self.settings.Config.env_prefix:
                    base_dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    base_dest = self.dest
                if option_string in self.option_strings:
                    if update:
                        setattr(
                            self.settings,
                            base_dest,
                            not option_string.startswith("--no-"),
                        )
                    else:
                        setattr(namespace, dest, not option_string.startswith("--no-"))

            def format_usage(self):  # type: ignore
                return " | ".join(self.option_strings)

        class SetSettingsAction(Action):
            settings = settings_ref

            def __call__(self, parser, namespace, values, option_string=None):  # type: ignore
                dest = self.dest
                if self.settings.Config.env_prefix:
                    base_dest = self.dest[len(self.settings.Config.env_prefix) :]
                else:
                    base_dest = self.dest
                if update and hasattr(self.settings, base_dest):
                    setattr(self.settings, base_dest, values)
                else:
                    setattr(namespace, dest, values)

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

        options, unknown = parser.parse_known_args(args)
        return options, unknown


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
