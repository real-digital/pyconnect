from pyconnect.core import PyConnectException
import datetime as dt
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


def call_if_string(parser: Callable[[str], Any]) -> Callable:

    def optional_parser(field):
        if isinstance(field, str):
            return parser(field)
        return field

    return optional_parser


def timedelta_parser(field: str) -> dt.timedelta:
    unit_map = {
        'ms': 'milliseconds',
        'us': 'microseconds',
        's': 'seconds',
        'h': 'hours',
        'd': 'days',
        'w': 'weeks'
    }

    matches = re.findall(r'(?P<unit_value>\d+)(?P<unit_key>ms|us|s|h|d|w)', field)
    return dt.timedelta(**{
        unit_map[match['unit_key']]: int(match['unit_value'])
        for match in matches
    })


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


class BaseConfig(dict):
    """
    :param bootstrap_servers: Servers used for establishing connection to Kafka cluster.
                              String with servers separated by ',' may be given.
                              Any quotes or whitespaces will be stripped
    :type bootstrap_servers: List[str]

    :param schema_registry: Server holding schema-information for avro data conversion.
                            See https://github.com/confluentinc/schema-registry
    :type schema_registry: str

    :param offset_commit_interval: Interval after which a Source or Sink shall commit its offsets to Kafka.
                                   May be `None`, then only `batch_size` is used.
                                   Interval may be given as a string in the form `'1h 30m'` meaning one hour and
                                   30 minutes.
                                   Valid units are *us = microseconds*, *ms = milliseconds*, *s = seconds*,
                                   *m = minutes*, *h = hours*, *d = days*, *w = weeks*.
                                   **Default is 30m**
    :type offset_commit_interval: datetime.timedelta

    :param kafka_opts: Additional options that shall be passed to the underlying Kafka library.
                       See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation.
    :type kafka_opts: Dict[str, str]
    """

    __sanity_checks = [
        '{offset_commit_interval}>0',
        check_field_is_valid_url('schema_registry'),
        check_field_is_valid_url('bootstrap_servers'),
    ]

    __parsers = {
        'bootstrap_servers': csv_line_reader(),
        'offset_commit_interval': timedelta_parser,
        'kafka_opts': json.loads
    }

    def __init__(self, conf_dict: dict):
        super().__init__()
        self['bootstrap_servers'] = conf_dict.pop('bootstrap_servers')
        self['schema_registry'] = conf_dict.pop('schema_registry')
        self['offset_commit_interval'] = conf_dict.pop('offset_commit_interval', '30m')
        self['kafka_opts'] = conf_dict.pop('kafka_opts', {})

        if len(conf_dict) != 0:
            raise TypeError(f'The following options are unused: {conf_dict!r}')

        self._apply_parsers()
        self._perform_sanity_checks()

    def _apply_parsers(self) -> None:
        parsers = self._find_parsers()
        for field, parser in parsers.items():
            value = self[field]
            if not isinstance(value, str):
                continue
            parsed_value = parser(value)
            self[field] = parsed_value

    def _find_parsers(self) -> Dict[str, Callable]:
        parsers: Dict[str, Callable] = {}

        for cls in self._find_subclasses():
            attr_name = f'_{cls.__name__}__parsers'
            if hasattr(self, attr_name):
                parsers.update(getattr(self, attr_name))
        return parsers

    def _find_subclasses(self):
        subclasses: List[type] = []
        for cls in inspect.getmro(type(self)):
            if issubclass(cls, BaseConfig):
                subclasses.append(cls)
        return subclasses

    def _perform_sanity_checks(self) -> None:
        logger.debug(f'Performing sanity checks on {self}')
        all_checks = self._find_sanity_checks()
        logger.debug(f'Found {len(all_checks)} sanity checks!')

        for check in all_checks:
            if isinstance(check, str):
                checker = _checkstr_to_checker(check)
            else:
                checker = check
            checker(self)
        logger.info('Config checks out sane!')

    def _find_sanity_checks(self) -> List[SanityCheck]:
        checks: List[SanityCheck] = []

        for cls in self._find_subclasses():
            attr_name = f'_{cls.__name__}__sanity_checks'
            if hasattr(self, attr_name):
                checks.extend(getattr(self, attr_name))
        return checks

    @classmethod
    def from_yaml_file(cls: Type['BaseConfig'],
                       yaml_file: str) -> 'BaseConfig':
        with open(yaml_file, 'r') as infile:
            conf = yaml.load(infile)
        return cls(conf)

    @classmethod
    def from_json_file(cls: Type['BaseConfig'],
                       json_file: str) -> 'BaseConfig':
        with open(json_file, 'r') as infile:
            conf = json.load(infile)
        return cls(conf)

    @classmethod
    def from_json_string(cls: Type['BaseConfig'],
                         json_string: str) -> 'BaseConfig':
        conf = json.loads(json_string)
        return cls(conf)

    @classmethod
    def from_env_variables(cls: Type['BaseConfig']) -> 'BaseConfig':
        conf = {
            key[len('PYCONNECT_'):]: value
            for key, value in os.environ
            if 'PYCONNECT_' in key
        }

        return cls(conf)  # type: ignore


class SinkConfig(BaseConfig):
    """
    :param :
    """

    __parsers = {
        'poll_timeout': int,
        'topics': csv_line_reader()
    }
    __sanity_checks = [
        '{poll_timeout}==-1 or {poll_timeout}>0'
    ]

    def __init__(self, conf_dict):
        self['group_id'] = conf_dict.pop('group_id')
        self['topics'] = conf_dict.pop('topics')
        self['poll_timeout'] = conf_dict.pop('poll_timeout', 2)

        super().__init__(conf_dict)


class SourceConfig(BaseConfig):

    def __init__(self, conf_dict):
        self['topic'] = conf_dict.pop('topic')
        self['offset_topic'] = conf_dict.pop('offset_topic', '_pyconnect_offsets')

        super().__init__(conf_dict)