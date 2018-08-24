from pyconnect.core import PyConnectException
import dataclasses
import ast
from typing import List, Dict, Callable, Any, Union, Type, Pattern, ClassVar
import json
import yaml
import os
import re
import logging
import inspect
logger = logging.getLogger(__name__)

SanityChecker = Callable[[Dict[str, Any]], None]
SanityCheck = Union[str, SanityChecker]


class ConfigException(PyConnectException):
    pass


class SanityError(ConfigException):
    pass


def check_field_matches_pattern(field: str, pattern: Union[str, Pattern]):
    def regex_checker(field_values):
        value = field_values[field]
        logger.debug('Validating field %r with Pattern %s', field, pattern)

        if not isinstance(value, (tuple, list)):
            value = [value]

        for str_ in value:
            logger.debug('Validating string %r.', str_)
            if pattern.match(str_) is None:
                logger.debug('Validation failed')
                raise SanityError(f'String {str_!r} does not match {pattern}')

        if len(value) > 1:
            logger.debug('String is valid!')
        else:
            logger.debug('Strings are valid!')

    return regex_checker


def check_field_is_valid_url(field: str) -> SanityChecker:
    pattern = re.compile(
        r'^(?:(?:http|ftp)s?:\/\/)?'                       # protocol
        r'(?:\w+?:.+?@)?'                                 # user:password
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+'  # domain...
        r'(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'             # ...
        r'[A-Z0-9\-]+|'                                    # host...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'             # ...or ip
        r'(?::\d+)?'                                       # optional port
        r'(?:/?|[\/?]\S+)$', re.IGNORECASE)                # path or params
    return check_field_matches_pattern(field, pattern)


def _validate_ast_tree(tree):
    valid_nodes = (ast.cmpop, ast.Module, ast.Expr, ast.Compare, ast.Num,
                   ast.Str, ast.expr, ast.boolop, ast.NameConstant, ast.Call,
                   ast.Name, ast.Load, ast.List, ast.Tuple, ast.unaryop)

    valid_names = ('len')

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in valid_names:
            raise ValueError(f'Illegal node found: {node}')

        if not isinstance(node, valid_nodes):
            raise ValueError(f'Illegal node found: {node}')


def _checkstr_to_checker(sanity_check: str) -> SanityChecker:
    def checker(all_fields: Dict[str, Any]):
        logger.debug(f'Validting fields using {sanity_check!r}')
        checker_expression = sanity_check.format(**{
            key: repr(value)
            for key, value in all_fields.items()})
        logger.debug(f'Formatted expression is {checker_expression!r}')

        tree = ast.parse(checker_expression)
        _validate_ast_tree(tree)

        success = eval(checker_expression)
        if not success:
            raise SanityError(f'Sanity check {sanity_check!r} failed! '
                              f'Formatted expression: {checker_expression!r}')
        logger.debug('Check successful')

    return checker


def csv_line_reader(separator=',', quoter='"', escaper='\\',
                    strip_chars='\t\n ') -> Callable[[str], List[str]]:

    def line_reader(input_line: str):
        charlist = list(input_line)
        fields = []
        quoting = False
        escaping = False

        if len(charlist) > 0:
            fields.append('')

        while len(charlist) > 0:
            char = charlist.pop(0)
            if escaping:
                fields[-1] += char
                escaping = False
                continue

            if char == escaper:
                escaping = True
            elif char == quoter:
                quoting = not quoting
            elif char == separator and not quoting:
                fields.append('')
            else:
                fields[-1] += char
        return [field.strip(strip_chars) for field in fields]

    return line_reader


@dataclasses.dataclass
class BaseConfig:
    bootstrap_servers: List[str] = dataclasses.field(metadata={
        'parser': csv_line_reader()
    })
    """
    Servers to connect to. PyConnect will cycle trough them and try each one
    until it can establish a connection. Other servers will be discovered via
    kafka metadata once a connection was successful.

    Servers are separated by `,`
    """

    schema_registry: str = dataclasses.field()
    """
    Schema registry server that holds the schema information which is used for
    message schema resolution.
    """

    flush_interval: float = dataclasses.field(metadata={
        'parser': float})
    """
    Interval in seconds after which a Source or Sink shall flush buffered
    messages and/or commit its offset.
    """

    __sanity_checks: ClassVar[List[SanityCheck]] = [
        '{flush_interval}>0',
        check_field_is_valid_url('schema_registry'),
        check_field_is_valid_url('bootstrap_servers'),
    ]

    def __post_init__(self) -> None:
        self._apply_parsers()
        self._perform_sanity_checks()

    def _apply_parsers(self) -> None:
        for field in dataclasses.fields(self):
            if field.metadata is None or 'parser' not in field.metadata:
                continue
            parser = field.metadata['parser']
            value = getattr(self, field.name)
            parsed_value = parser(value)
            setattr(self, field.name, parsed_value)

    def _find_sanity_checks(self) -> List[SanityCheck]:
        checks: List[SanityCheck] = []
        for cls in inspect.getmro(type(self)):
            if issubclass(cls, BaseConfig):
                attr = f'_{cls.__name__}__sanity_checks'
                checks.extend(getattr(cls, attr, []))
        return checks

    def _perform_sanity_checks(self) -> None:
        logger.debug(f'Performing sanity checks on {self}')
        all_checks = self._find_sanity_checks()
        logger.debug(f'Found {len(all_checks)} sanity checks!')

        field_values: Dict[str, Any] = dataclasses.asdict(self)
        for check in all_checks:
            if isinstance(check, str):
                checker = _checkstr_to_checker(check)
            else:
                checker = check
            checker(field_values)
        logger.info('Config checks out sane!')

    @classmethod
    def from_yaml_file(cls: Type['BaseConfig'],
                       yaml_file: str) -> 'BaseConfig':
        with open(yaml_file, 'r') as infile:
            conf = yaml.load(infile)
        return cls(**conf)

    @classmethod
    def from_json_file(cls: Type['BaseConfig'],
                       json_file: str) -> 'BaseConfig':
        with open(json_file, 'r') as infile:
            conf = json.load(infile)
        return cls(**conf)

    @classmethod
    def from_json_string(cls: Type['BaseConfig'],
                         json_string: str) -> 'BaseConfig':
        conf = json.loads(json_string)
        return cls(**conf)

    @classmethod
    def from_env_variables(cls: Type['BaseConfig']) -> 'BaseConfig':
        fields = dataclasses.fields(cls)
        env_vars = [
            'PYCONNECT_'+field.name.upper()
            for field in fields
        ]

        conf = {
            field.name: os.environ[env_var]
            for field, env_var in zip(fields, env_vars)
            if env_var in os.environ
        }

        return cls(**conf)  # type: ignore


@dataclasses.dataclass
class SinkConfig(BaseConfig):
    group_id: str = dataclasses.field()

    topics: List[str] = dataclasses.field(metadata={
        'parser': csv_line_reader()})

    poll_timeout: int = dataclasses.field(default=60, metadata={
        'parser': int})

    consumer_options: Dict[str, str] = dataclasses.field(default_factory=dict)

    __sanity_checks: ClassVar[List[SanityCheck]] = [
        '{poll_timeout}==-1 or {poll_timeout}>0'
    ]


@dataclasses.dataclass
class SourceConfig(BaseConfig):
    topic: str = dataclasses.field()
    offset_topic: str = '_pyconnect_offsets'
