"""
This module contains the classes which are used as configurations for the connectors. The respective configuration
classes can be subclassed to extend them with additional fields and sanity checks.
"""
import builtins
import datetime
import json
import logging
import os
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Type, Union

import yaml
from loguru import logger
from pyconnect.errors import SinkConfigError
from pydantic import AnyHttpUrl, BaseSettings, validator


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def configure_logging(use_stderr=False) -> None:
    """
    Configure logging so every log, exception, warning or print goes through loguru. This makes sure we only get
    messages that are formatted the way we want.

    :param use_stderr: Use stderr instead of stdout. Useful when cli commands are used in a pipe and need to provide
                       output in a certain format
    """
    handlers: List[Dict] = [
        {
            "sink": sys.stderr if use_stderr else sys.stdout,
            "format": "{time} | {level} | {thread.name}:{name}:{function}:{line} | {message}",
            "serialize": True,
        }
    ]

    requested_level = os.getenv("LOGURU_LEVEL")
    if requested_level:
        handlers[0]["level"] = requested_level
    else:
        handlers[0]["level"] = "DEBUG"

    requested_colorize = os.getenv("LOGURU_COLORIZE")
    if requested_colorize:
        colorize = requested_colorize.lower() not in ("0", "f", "n", "false", "no")
        handlers[0]["colorize"] = colorize
    else:
        handlers[0]["colorize"] = False

    logger.configure(handlers=handlers)

    warnings.showwarning = loguru_showwarning
    builtins.print = loguru_print_override
    sys.excepthook = loguru_excepthook
    # noinspection PyArgumentList
    logging.basicConfig(handlers=[InterceptHandler()], level=0)


original_print = builtins.print


def loguru_print_override(*args, sep=" ", end="\n", file=None):
    if file not in (None, sys.stderr, sys.stdout):
        original_print(*args, sep=sep, end=end, file=file)
    msg = sep.join(map(str, args))
    logger.opt(depth=1).info(msg)


def loguru_excepthook(exctype, value, traceback):
    logger.opt(exception=(exctype, value, traceback)).error("Unhandled Exception Occurred!")
    sys.exit(1)


def loguru_showwarning(message, category, filename, lineno, file=None, line=None):
    warning_msg = f"{category.__name__}: {message}"
    logger.opt(depth=2).warning(warning_msg)


class BaseConfig(BaseSettings):
    """
    This class represents the basic configuration object containing all config variables that are needed by every
    connector such as kafka server urls or commit intervals.

    It offers two additional functionalities:

        1. Parameter parsing:
           A subclass can use a private variable `__parsers` of type Dict[str, Callable[[str], Any] where the keys
           correspond to configuration field names and the value is a function that parses a given string (coming from
           an environment variable or yaml file) to the desired type of that value.
           *Parsers will only be applied if the config value is a string*

        2. Sanity checking:
           A subclass can use a private variable `__sanity_checks` of type List[SanityCheck] where a sanity check may
           either be a string of the form `'{variable_name} > 0 or {variable_name} < {other_variable_name}'
           (see :func:`pyconnect.config._checkstr_to_checker`) or a Callable[[BaseConfig], None] that raises
           :class:`pyconnect.config.SanityError` if the check fails.

    .. note::
       Subclasses have to make sure to call the super constructor *at the end* of their own constructor.
       See :class:`pyconnect.config.SinkConfig` for an exemplary implementation.

    This base config offers the following fields:

        **bootstrap_servers**: List[str]
            Servers used for establishing connection to Kafka cluster.
            String with servers separated by ', ' may be given. Any quotes or whitespaces will be stripped.

        **schema_registry**: str
            Server holding schema-information for avro data conversion.
            See https://github.com/confluentinc/schema-registry

        **offset_commit_interval**: datetime.timedelta
            Interval may be even given as an integer in seconds o in ISO8601 format for durations.
            See https://en.wikipedia.org/wiki/ISO_8601#Durations
            *Default is 30 minutes*

        **sink_commit_retry_count**: int
            The number of retries for the Sink when committing offsets.
            Default: 2

        **hash_sensitive_values**: bool
            If true, sensitive values (e.g. sasl.password) from the kafka_opts configurations are
            hashed and logged with the hashing parameters, so that the values can be validated.
            If false, the sensitive values are replaced by "****", offering no opprotunity to validate.
            Default is true.

        **kafka_opts**: Dict[str, str]
            Additional options that shall be passed to the underlying Kafka library.
            See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation.

        **unify_logging**: bool
            Use a common logging JSON format and intercept all logging messages through loguru.
            Default is true.
    """

    bootstrap_servers: List[str]
    schema_registry: AnyHttpUrl
    offset_commit_interval: datetime.timedelta = datetime.timedelta(minutes=30)
    sink_commit_retry_count: int = 2
    hash_sensitive_values: bool = True
    kafka_opts: Optional[Dict[str, str]] = None
    unify_logging: bool = False

    class Config:
        env_prefix = "pyconnect_"

    @classmethod
    def from_yaml_file(cls: Type["BaseConfig"], yaml_file: Union[str, Path]) -> "BaseConfig":
        """
        Loads a yaml file and uses it to create the config
        """
        with open(yaml_file, "r") as infile:
            conf = yaml.safe_load(infile)
        return cls(**conf)

    @classmethod
    def from_json_file(cls: Type["BaseConfig"], json_file: Union[str, Path]) -> "BaseConfig":
        """
        Loads a json file and uses it to create the config
        """
        with open(json_file, "r") as infile:
            conf = json.load(infile)
        return cls(**conf)

    @classmethod
    def from_json_string(cls: Type["BaseConfig"], json_string: str) -> "BaseConfig":
        """
        Takes a json string, parses it and then creates the config from it
        """
        conf = json.loads(json_string)
        return cls(**conf)


class SinkConfig(BaseConfig):
    """
    Configuration needed for :class:`pyconnect.pyconnectsink.PyConnectSink` objects. In addition to those
    from :class:`pyconnect.config.BaseConfig` it offers the following fields:

        **group_id**: str
            The kafka consumer group id this sink shall use.
        **topics**: List[Union[str, regex]]
            List of topics this sink shall subscribe to. If a topic starts with `'^'`, it is considered a regex
            expression.
        **poll_timeout**: float
            Maximum amount of time *in seconds* to wait for the next message, before continuing with the loop.
            If set to `-1` there will be no timeout.
            *Default is 2*
    """

    group_id: str
    topics: List[str]
    poll_timeout: float = 2.0

    @validator("poll_timeout")
    def check_poll_timeout(cls, value):
        if (value != -1.0) and not (value > 0.0):
            raise SinkConfigError("Poll Timeout of {} invalid can only be be > 0 or -1".format(value))


class SourceConfig(BaseConfig):
    """
    Configuration needed for :class:`pyconnect.pyconnectsource.PyConnectSource` objects. In addition to those
    from :class:`pyconnect.config.BaseConfig` it offers the following fields:

        **topic**: str
            The kafka topic where the pyconnect source shall publish to
        **offset_topic**: str
            The kafka topic where this pyconnect source will safe its source offsets to.
    """

    topic: str
    offset_topic: str
