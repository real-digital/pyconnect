"""
This module contains the classes which are used as configurations for the connectors. The respective configuration
classes can be subclassed to extend them with additional fields and sanity checks.
"""
import ast
import datetime as dt
import inspect
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Pattern, Type, Union

import yaml

from .core import PyConnectException

logger = logging.getLogger(__name__)

SanityChecker = Callable[["BaseConfig"], None]
SanityCheck = Union[str, SanityChecker]


class ConfigException(PyConnectException):
    """
    Base exception for all exceptions raised by a configuration class.
    """

    pass


class SanityError(ConfigException):
    """
    This exception is raised whenever a config parameter sanity check fails.
    """

    pass


def timedelta_parser(field: str) -> dt.timedelta:
    """
    Takes a string of the form '1h 30m' and translates it to a :class:`datetime.timedelta` object.
    Valid units are *us = microseconds*, *ms = milliseconds*, *s = seconds*, *m = minutes*, *h = hours*, *d = days*,
    *w = weeks*.
    Floating point numbers are not supported, order of the units is irrelevant.

    >>> import datetime as dt
    >>> from pyconnect.config import timedelta_parser
    >>> delta = timedelta_parser('1h 30m')
    >>> assert delta == dt.timedelta(hours=1, minutes=30)

    :param field: The string representing time delta information.
    :return: The parsed time delta.
    """

    unit_map = {
        "ms": "milliseconds",
        "us": "microseconds",
        "m": "minutes",
        "s": "seconds",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    matches = re.findall(r"(\d+)(us|ms|s|m|h|d|w)", field)
    return dt.timedelta(**{unit_map[unit_key]: int(unit_value) for unit_value, unit_key in matches})


def check_field_is_valid_url(field: str) -> SanityChecker:
    """
    Creates a checker that assures that the configuration field with name `field` has only values that are a valid
    url.
    Meant for use in :attr:`pyconnect.config.BaseConfig.__sanity_checks`.

    >>> from pyconnect.config import check_field_matches_pattern
    >>> from typing import cast
    >>> url_checker = check_field_is_valid_url('test')
    >>> url_checker(cast(BaseConfig, dict(test='http://myurl.com')))
    >>> url_checker(cast(BaseConfig, dict(test='user:password@some.other.url.com')))
    >>> url_checker(cast(BaseConfig, dict(test='invalid_http://_url')))  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    pyconnect.config.SanityError: String 'invalid_http://_url' does not match (...)

    :param field: The field to check.
    """

    pattern = re.compile(
        r"^(?:(?:http|ftp)s?://)?"  # protocol
        r"(?:\w+?:.+?@)?"  # user:password
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+"  # domain...
        r"(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|"  # ...
        r"[A-Z0-9\-]+|"  # host...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )  # path or params
    return check_field_matches_pattern(field, pattern)


def check_field_matches_pattern(field: str, pattern: Union[str, Pattern]) -> SanityChecker:
    """
    Creates a checker that assures that the configuration field with name `field` has only values that match
    `pattern`.
    Meant for use in :attr:`pyconnect.config.BaseConfig.__sanity_checks`.

    >>> from pyconnect.config import check_field_matches_pattern
    >>> from typing import cast
    >>> pattern_checker = check_field_matches_pattern('test','^mypatternA?$')
    >>> pattern_checker(cast(BaseConfig, dict(test='mypattern')))
    >>> pattern_checker(cast(BaseConfig, dict(test='mypatternA')))
    >>> pattern_checker(cast(BaseConfig, dict(test='mypatternB')))
    Traceback (most recent call last):
    ...
    pyconnect.config.SanityError: String 'mypatternB' does not match re.compile('^mypatternA?$')

    :param field: The field to check.
    :param pattern: The pattern to match.
    """
    if isinstance(pattern, str):
        pattern = re.compile(pattern)

    def regex_checker(field_values):
        value = field_values[field]
        logger.debug("Validating field %r with Pattern %s", field, pattern)

        if not isinstance(value, (tuple, list)):
            value = [value]

        for str_ in value:
            logger.debug("Validating string %r.", str_)
            if pattern.match(str_) is None:
                logger.debug("Validation failed")
                raise SanityError(f"String {str_!r} does not match {pattern}")

        if len(value) > 1:
            logger.debug("String is valid!")
        else:
            logger.debug("Strings are valid!")

    return regex_checker


def _checkstr_to_checker(sanity_check: str) -> SanityChecker:
    """
    Turns a sanity check defined by a string into a proper sanity checker. Such a string may be for example
    `'{timeout_ms} > 0'` or `'len({bootstrap_servers}) > 0'`.
    Substrings of the form `'{variable_name}'` will be replaced by `repr(config['variable_name'])`.
    Although `eval` is used for evaluation of the result, only an extremely limited set of expressions is allowed.
    See :func:`pyconnect.config._validate_ast_tree`.

    >>> from pyconnect.config import _checkstr_to_checker
    >>> from typing import cast
    >>> checker = _checkstr_to_checker('{timeout_ms} > 0')
    >>> checker(cast(BaseConfig, dict(timeout_ms=10)))
    >>> checker(cast(BaseConfig, dict(timeout_ms=-1)))
    Traceback (most recent call last):
    ...
    pyconnect.config.SanityError: Sanity check '{timeout_ms} > 0' failed! Formatted expression: '-1 > 0'

    :param sanity_check: String defining sanity check.
    """

    def checker(all_fields: BaseConfig) -> None:
        logger.debug(f"Validting fields using {sanity_check!r}")
        checker_expression = sanity_check.format(
            **{
                key: repr(value.total_seconds()) if isinstance(value, dt.timedelta) else repr(value)
                for key, value in all_fields.items()
            }
        )
        logger.debug(f"Formatted expression is {checker_expression!r}")

        tree = ast.parse(checker_expression)
        _validate_ast_tree(tree)

        success = eval(checker_expression)
        if not success:
            raise SanityError(
                f"Sanity check {sanity_check!r} failed! " f"Formatted expression: {checker_expression!r}"
            )
        logger.debug("Check successful")

    return checker


def _validate_ast_tree(tree: ast.AST) -> None:
    """
    Security function which checks whether a given tree only contains valid nodes. We're taking a whitelisting
    approach here to make sure that there is no way to execute malicious code. We only want to be able to evaluate
    comparisons of literals and the `len` function.
    If this validation fails, the string corresponding to `tree` will not be evaluated.

    :param tree: The tree to check for invalid nodes.
    """
    valid_nodes = (
        ast.cmpop,
        ast.Module,
        ast.Expr,
        ast.Compare,
        ast.Num,
        ast.Str,
        ast.expr,
        ast.boolop,
        ast.NameConstant,
        ast.Call,
        ast.Name,
        ast.Load,
        ast.List,
        ast.Tuple,
        ast.unaryop,
    )

    valid_names = "len"

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in valid_names:
            raise ValueError(f"Illegal node found: {node}")

        if not isinstance(node, valid_nodes):
            raise ValueError(f"Illegal node found: {node}")


def csv_line_reader(separator=",", quoter='"', escaper="\\", strip_chars="\r\t\n ") -> Callable[[str], List[str]]:
    """
    Creates a function that parses a **line** in csv format using the given parameters and returns a list of strings.

    .. warning::
       Quoting a field does not prevent leading and trailing chars to be stripped.
       Therefore parsing `'" here I ",am'` would result in `['here I', 'am']`.

    >>> from pyconnect.config import csv_line_reader
    >>> reader = csv_line_reader()
    >>> line = r'"quoted,field",escaped\,field, stripped field ," quoted \" escaped field"'
    >>> reader(line)
    ['quoted,field', 'escaped,field', 'stripped field', 'quoted " escaped field']

    :param separator: Char used to separate fields.
    :param quoter: Char used to quote a field, separators are ignored within quoted fields.
    :param escaper: Char used to escape to following char, for example when it is a separator.
    :param strip_chars: Those chars are stripped from the beginning and end of each field.
    :return: A parser for csv lines.
    """  # noqa: W605

    def line_reader(input_line: str):
        charlist = list(input_line)
        fields = []
        quoting = False
        escaping = False

        if len(charlist) > 0:
            fields.append("")

        while len(charlist) > 0:
            char = charlist.pop(0)
            if escaping:
                fields[-1] += char
                escaping = False
            elif char == escaper:
                escaping = True
            elif char == quoter:
                quoting = not quoting
            elif char == separator and not quoting:
                fields.append("")
            else:
                fields[-1] += char
        return [field.strip(strip_chars) for field in fields]

    return line_reader


class BaseConfig(dict):
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
            Interval after which a Source or Sink shall commit its offsets to Kafka.
            May be `None`, then only `batch_size` is used.
            Interval may be given as a string in the form `'1h 30m'` meaning one hour and
            30 minutes. See :func:`pyconnect.config.timedelta_parser` for more info.
            *Default is 30m*

        **kafka_opts**: Dict[str, str]
            Additional options that shall be passed to the underlying Kafka library.
            See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation.
    """

    __sanity_checks = [
        "{offset_commit_interval}>0",
        check_field_is_valid_url("schema_registry"),
        check_field_is_valid_url("bootstrap_servers"),
    ]

    __parsers = {
        "bootstrap_servers": csv_line_reader(),
        "offset_commit_interval": timedelta_parser,
        "kafka_opts": json.loads,
    }

    def __init__(self, conf_dict: Dict[str, Any]) -> None:
        super().__init__()
        self["bootstrap_servers"] = conf_dict.pop("bootstrap_servers")
        self["schema_registry"] = conf_dict.pop("schema_registry")
        self["offset_commit_interval"] = conf_dict.pop("offset_commit_interval", "30m")
        self["kafka_opts"] = conf_dict.pop("kafka_opts", {})

        if len(conf_dict) != 0:
            raise TypeError(f"The following options are unused: {conf_dict!r}")

        self._apply_parsers()
        self._perform_sanity_checks()

    def _apply_parsers(self) -> None:
        """
        Apply all parsers that are defined on this object.
        Will go through all the base classes and inspect their `__parsers` attribute.
        """
        parsers = self._find_parsers()
        for field, parser in parsers.items():
            value = self[field]
            if not isinstance(value, str):
                continue
            parsed_value = parser(value)
            self[field] = parsed_value

    def _find_parsers(self) -> Dict[str, Callable]:
        """
        Find and combine all `__parsers` fields defined on the type hierarchy from `type(self)` up to BaseConfig
        """
        parsers: Dict[str, Callable] = {}

        for cls in self._find_subclasses():
            attr_name = f"_{cls.__name__}__parsers"
            if hasattr(self, attr_name):
                parsers.update(getattr(self, attr_name))
        return parsers

    def _find_subclasses(self) -> List[Type["BaseConfig"]]:
        """
        Find all classes that are parents of `type(self)` and subclasses of :class:`pyconnect.config.BaseConfig`.
        """
        subclasses: List[Type["BaseConfig"]] = []
        for cls in inspect.getmro(type(self)):
            if issubclass(cls, BaseConfig):
                subclasses.append(cls)
        return subclasses

    def _perform_sanity_checks(self) -> None:
        """
        Perform all sanity checks that are defined on this object.
        Will go through all the base classes and inspect their `__sanity_checks` attribute.
        """
        logger.debug(f"Performing sanity checks on {self}")
        all_checks = self._find_sanity_checks()
        logger.debug(f"Found {len(all_checks)} sanity checks!")

        for check in all_checks:
            if isinstance(check, str):
                checker = _checkstr_to_checker(check)
            else:
                checker = check
            checker(self)
        logger.info("Config checks out sane!")

    def _find_sanity_checks(self) -> List[SanityCheck]:
        """
        Find and combine all `__sanity_checks` fields defined on the type hierarchy from `type(self)` up to BaseConfig
        """
        checks: List[SanityCheck] = []

        for cls in self._find_subclasses():
            attr_name = f"_{cls.__name__}__sanity_checks"
            if hasattr(self, attr_name):
                checks.extend(getattr(self, attr_name))
        return checks

    @classmethod
    def from_yaml_file(cls: Type["BaseConfig"], yaml_file: Union[str, Path]) -> "BaseConfig":
        """
        Loads a yaml file and uses it to create the config
        """
        with open(yaml_file, "r") as infile:
            conf = yaml.load(infile)
        return cls(conf)

    @classmethod
    def from_json_file(cls: Type["BaseConfig"], json_file: Union[str, Path]) -> "BaseConfig":
        """
        Loads a json file and uses it to create the config
        """
        with open(json_file, "r") as infile:
            conf = json.load(infile)
        return cls(conf)

    @classmethod
    def from_json_string(cls: Type["BaseConfig"], json_string: str) -> "BaseConfig":
        """
        Takes a json string, parses it and then creates the config from it
        """
        conf = json.loads(json_string)
        return cls(conf)

    @classmethod
    def from_env_variables(cls: Type["BaseConfig"]) -> "BaseConfig":
        """
        Takes all environment variables, turns their keys to lowercase and checks if they start with `'pyconnect_'`.
        If so, it strips the prefix and adds them to a dictionary which is then used to create the config object.

        So for example if there was a single eviornment variable `PYCONNECT_BOOTSTRAP_SERVERS="myserver1,myserver2"` it
        would be transformed to the dictionary `{'bootstrap_servers': 'myserver1,myserver2'}` which would then be used
        to create the config object.
        """
        prefix = "pyconnect_"

        def strip_prefix(env_var_name: str) -> str:
            return env_var_name[len(prefix) :]

        conf = {strip_prefix(key).lower(): value for key, value in os.environ.items() if prefix in key.lower()}

        return cls(conf)


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

    __parsers = {"poll_timeout": (lambda x: float(x) if float(x) != -1 else None), "topics": csv_line_reader()}
    __sanity_checks = ["{poll_timeout}==-1 or {poll_timeout}>0"]

    def __init__(self, conf_dict: Dict[str, Any]) -> None:
        self["group_id"] = conf_dict.pop("group_id")
        self["topics"] = conf_dict.pop("topics")
        self["poll_timeout"] = conf_dict.pop("poll_timeout", 2)

        super().__init__(conf_dict)


class SourceConfig(BaseConfig):
    """
    Configuration needed for :class:`pyconnect.pyconnectsource.PyConnectSource` objects. In addition to those
    from :class:`pyconnect.config.BaseConfig` it offers the following fields:

        **topic**: str
            The kafka topic where the pyconnect source shall publish to
        **offset_topic**: str
            The kafka topic where this pyconnect source will safe its source offsets to.
    """

    def __init__(self, conf_dict: Dict[str, Any]) -> None:
        self["topic"] = conf_dict.pop("topic")
        self["offset_topic"] = conf_dict.pop("offset_topic")

        super().__init__(conf_dict)
