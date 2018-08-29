from typing import Any, List, Dict, Tuple
from unittest import mock
from confluent_kafka import Message, KafkaError
from confluent_kafka import avro as confluent_avro
from pprint import pprint
import itertools as it
import yaml
import subprocess
import pytest
import string
import random
import json
import os

from pyconnect.avroparser import create_schema_from_record
from pyconnect.config import SourceConfig
from pyconnect.core import Status
from pyconnect.pyconnectsink import PyConnectSink
from pyconnect.pyconnectsource import PyConnectSource

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
CLI_DIR = os.path.join(TEST_DIR, 'kafka', 'bin')


class TestException(Exception):
    pass


# noinspection PyUnresolvedReferences
class ConnectTestMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.forced_status_after_run = None
        self.run_counter = 0
        self.max_runs = 20
        self._ignore_crash = False

    def _run_loop(self):
        while self.is_running:
            if self.run_counter >= self.max_runs:
                pytest.fail('Runlimit Reached! Forgot to force stop?')
            self.run_counter += 1

            self._run_once()
            if self._status == Status.CRASHED and \
                    self._status_info is not None and \
                    not isinstance(self._status_info, TestException):
                raise self._status_info

            if isinstance(self.forced_status_after_run, list):
                if len(self.forced_status_after_run) > 1:
                    new_status = self.forced_status_after_run.pop(0)
                else:
                    new_status = self.forced_status_after_run[0]
            else:
                new_status = self.forced_status_after_run

            if new_status is not None:
                self._status = new_status

    def on_crashed(self):
        try:
            new_status = super().on_crashed()
        except Exception as e:
            new_status = e

        if hasattr(self, '_when_crashed'):
            new_status = self._when_crashed

        if isinstance(new_status, Exception):
            raise new_status
        return new_status

    def on_shutdown(self):
        super().on_shutdown()
        if self.status == Status.CRASHED and self._ignore_crash:
            self._status = Status.STOPPED

    def when_crashed(self, return_value) -> 'ConnectTestMixin':
        self._when_crashed = return_value
        return self

    def ignoring_crash_on_shutdown(self) -> 'ConnectTestMixin':
        self._ignore_crash = True
        return self

    def with_wrapper_for(self, func: str) -> 'ConnectTestMixin':
        old_func = getattr(self, func)
        setattr(self, func, mock.Mock(name=func, wraps=old_func))
        return self

    def with_mock_for(self, func: str) -> 'ConnectTestMixin':
        setattr(self, func, mock.Mock(name=func))
        return self

    def with_function_raising_after_n_calls(self, funcname: str,
                                            exception: Exception, n_calls: int) -> 'ConnectTestMixin':
        counter = 0
        original_function = getattr(self, funcname)

        def wrapper_function(*args, **kwargs):
            nonlocal counter
            if counter == n_calls:
                raise exception
            counter += 1
            return original_function(*args, **kwargs)

        setattr(self, funcname, wrapper_function)
        return self

    def with_function_returning_after_n_calls(self, funcname: str,
                                              return_value: Any, n_calls: int) -> 'ConnectTestMixin':
        counter = 0
        original_function = getattr(self, funcname)

        def wrapper_function(*args, **kwargs):
            nonlocal counter
            if counter == n_calls:
                return return_value
            counter += 1
            return original_function(*args, **kwargs)

        setattr(self, funcname, wrapper_function)
        return self


class PyConnectTestSource(ConnectTestMixin, PyConnectSource):

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self.records: List[Any] = []
        self.idx = 0
        self._when_eof = Status.STOPPED

    def on_eof(self):
        if isinstance(self._when_eof, Exception):
            raise self._when_eof
        return self._when_eof

    def with_records(self, records: List[Tuple[Any, Any]]) -> 'PyConnectTestSource':
        self.records = records
        return self

    def when_eof(self, return_value) -> 'PyConnectTestSource':
        self._when_eof = return_value
        return self

    def with_committed_offset(self, offset: Any) -> 'PyConnectTestSource':
        # noinspection PyAttributeOutsideInit
        self._committed_offset = offset
        return self

    def _get_committed_offset(self):
        if hasattr(self, '_committed_offset'):
            return self._committed_offset
        return super()._get_committed_offset()

    def seek(self, idx: int):
        if idx is None:
            self.idx = 0
        self.idx = idx

    def read(self) -> Any:
        try:
            record = self.records[self.idx]
        except IndexError:
            raise StopIteration()
        self.idx += 1
        return record

    def get_index(self):
        return self.idx


class PyConnectTestSink(ConnectTestMixin, PyConnectSink):

    def __init__(self, sink_config) -> None:
        self.message_buffer: List[Tuple[Any, Any]] = []
        self.flushed_messages: List[Tuple[Any, Any]] = []
        self.flush_interval = 5
        super().__init__(sink_config)

    def on_message_received(self, msg: Message) -> None:
        print(f'Message received: {message_repr(msg)}')
        # noinspection PyArgumentList
        self.message_buffer.append((msg.key(), msg.value()))

    def _check_status(self):
        print('Kafka consumer group status:')
        subprocess.call([
            os.path.join(CLI_DIR, 'kafka-consumer-groups.sh'),
            '--bootstrap-server', self.config['bootstrap_servers'][0],
            '--describe', '--group', self.config['group_id'],
            '--offsets', '--verbose'
        ])

    def on_startup(self):
        print('######## CONSUMER STARUP #########')
        print(f'Config: {self.config!r}')
        self._check_status()

    def need_flush(self):
        return len(self.message_buffer) == self.flush_interval

    def on_flush(self) -> None:
        print('Flushing messages:')
        pprint(self.message_buffer)
        self.flushed_messages.extend(self.message_buffer)
        self.message_buffer.clear()

    def on_shutdown(self) -> None:
        print('######## CONSUMER SHUTDOWN #########')
        self._check_status()
        print('----\nFlushed messages:')
        pprint(self.flushed_messages)

    def on_no_message_received(self):
        if self.eof_reached != {} and all(self.eof_reached.values()):
            return Status.STOPPED
        return None


def rand_text(textlen):
    return ''.join(random.choices(string.ascii_uppercase, k=textlen))


def to_schema(name: str, record: Any):
    return confluent_avro.loads(json.dumps(
        create_schema_from_record(name, record)))


def message_repr(msg: Message):
    return (
        f'Message(key={msg.key()!r}, value={msg.value()!r}, '
        f'topic={msg.topic()!r}, partition={msg.partition()!r}, '
        f'offset={msg.offset()!r}, error={msg.error()!r})'
    )


@pytest.fixture(params=[Status.CRASHED, TestException()],
                ids=['Status_CRASHED', 'TestException'])
def failing_callback(request):
    return mock.Mock(side_effect=it.repeat(request.param))


@pytest.fixture(scope='module')
def cluster_hosts():
    with open(os.path.join(TEST_DIR,
                           'testenv-docker-compose.yml'), 'r') as infile:
        yml_config = yaml.load(infile)

    hosts = {}
    for service, conf in yml_config['services'].items():
        port = conf['ports'][0].split(':')[0]
        hosts[service] = f'{service}:{port}'

    hosts['schema-registry'] = 'http://'+hosts['schema-registry']
    hosts['rest-proxy'] = 'http://'+hosts['rest-proxy']

    completed = subprocess.run(
        ['curl', '-s', hosts['rest-proxy'] + "/topics"],
        stdout=subprocess.DEVNULL)

    if completed.returncode != 0:
        pytest.fail('Kafka Cluster is not running!')

    return hosts


@pytest.fixture(
    params=[1, 2, 4],
    ids=['num_partitions=1', 'num_partitions=2', 'num_partitions=4'])
def topic(request, cluster_hosts: Dict[str, str]):
    topic_id = rand_text(5)
    partitions = request.param

    subprocess.call([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--create', '--topic', topic_id,
        '--partitions', str(partitions),
        '--replication-factor', '1'
    ])

    yield (topic_id, partitions)

    subprocess.call([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--describe', '--topic', topic_id
    ])


@pytest.fixture
def message_factory():
    with mock.patch('test.utils.Message', autospec=True):
        def message_factory_(key='key', value='value', topic='topic', offset=0, partition=0, error=None):
            msg = Message()
            msg.error.return_value = error
            msg.topic.return_value = topic
            msg.partition.return_value = partition
            msg.offset.return_value = offset
            msg.key.return_value = key
            msg.value.return_value = value
            return msg
        yield message_factory_


@pytest.fixture
def error_message_factory(message_factory):
    with mock.patch('test.utils.KafkaError', autospec=True):
        def error_message_factory_(error_code=None):
            error = KafkaError()
            error.code.return_value = error_code
            return message_factory(error=error)

        yield error_message_factory_


@pytest.fixture
def eof_message(error_message_factory):
    return error_message_factory(error_code=KafkaError._PARTITION_EOF)
