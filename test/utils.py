"""
This module contains utility functions and classes for testing.
"""
import itertools as it
import os
from pprint import pprint
import random
import string
import subprocess
from typing import Any, Dict, List, Optional, Tuple, Callable
from unittest import mock

from confluent_kafka import KafkaError, Message
import pytest
import yaml

from pyconnect.config import SourceConfig
from pyconnect.core import Status
from pyconnect.pyconnectsink import PyConnectSink
from pyconnect.pyconnectsource import PyConnectSource

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
CLI_DIR = os.path.join(TEST_DIR, 'kafka', 'bin')


class TestException(Exception):
    """
    Special exception used to make sure that it really IS the exception we raised that is propagated.

    Consider mocking a method with `side_effect=Exception()` vs `side_effect=TestException()`.
    You'd need `with pytest.raises(Exception):` vs. `with pytest.raises(TestException):`, where the former would pass
    even for TypeErr or ValueError that are not necessarily caused by the mock.
    """
    pass


# noinspection PyUnresolvedReferences,PyAttributeOutsideInit
class ConnectTestMixin:
    """
    Mixin class meant to stand between connector's abstract class and its implementing class. It will intercept
    or wrap some methods so we can cause certain behaviour and can make sure not to end in an endless loop.
    It will also add some utility methods so the desired behaviour is more easily configurable.
    """

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
        if self.status == Status.CRASHED and self._ignore_crash:
            self._status = Status.STOPPED
        super().on_shutdown()

    def when_crashed(self, return_value) -> 'ConnectTestMixin':
        """
        Sets fixed a return value for :meth:`pyconnect.core.BaseConnector.on_crash`.
        If the value is an Exception, it will be raised.

        Returns the connector so the method can be chained with others.
        :param return_value: The value or exception to be returned
                             or raised by :meth:`pyconnect.core.BaseConnector.on_crash`.
        :return: self
        """
        self._when_crashed = return_value
        return self

    def with_wrapper_for(self, func: str) -> 'ConnectTestMixin':
        """
        Creates a mock that wraps the given method.

        Returns the connector so the method can be chained with others.
        :param func: Name of the method that shall be wrapped.
        :return: self
        """
        old_func = getattr(self, func)
        setattr(self, func, mock.Mock(name=func, wraps=old_func))
        return self

    def with_mock_for(self, func: str) -> 'ConnectTestMixin':
        """
        Creates a mock that replaces the given method.

        Returns the connector so the method can be chained with others.
        :param func: Name of the method that shall be wrapped.
        :return: self
        """
        setattr(self, func, mock.Mock(name=func))
        return self

    def with_method_raising_after_n_calls(self, methname: str,
                                          exception: Exception, n_calls: int) -> 'ConnectTestMixin':
        """
        Makes the method `methname` raise the exception `exception` after it has been called `n_called` times.

        Returns the connector so the method can be chained with others.
        :param methname: The name of the method to be changed.
        :param exception: The exception to be raised.
        :param n_calls: The number of calls to wait for.
        :return: self
        """
        counter = 0
        original_function = getattr(self, methname)

        def wrapper_function(*args, **kwargs):
            nonlocal counter
            if counter == n_calls:
                raise exception
            counter += 1
            return original_function(*args, **kwargs)

        setattr(self, methname, wrapper_function)
        return self

    def with_method_returning_after_n_calls(self, methname: str,
                                            return_value: Any, n_calls: int) -> 'ConnectTestMixin':
        """
        Makes the method `methname` return the value `return_value` after it has been called `n_called` times.

        Returns the connector so the method can be chained with others.
        :param methname: The name of the method to be changed.
        :param return_value: The value to be returned.
        :param n_calls: The number of calls to wait for.
        :return: self
        """
        counter = 0
        original_function = getattr(self, methname)

        def wrapper_function(*args, **kwargs):
            nonlocal counter
            if counter == n_calls:
                return return_value
            counter += 1
            return original_function(*args, **kwargs)

        setattr(self, methname, wrapper_function)
        return self


class PyConnectTestSource(ConnectTestMixin, PyConnectSource):
    """
    Implementation of a PyConnectSource that uses a list of records (i.e tuples of key, value items) to produce
    messages to the configured topic.

    Its default behaviour is to stop when it hits the end of its record list.
    """

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self.records: List[Tuple[Any, Any]] = []
        self.idx = 0
        self._when_eof = Status.STOPPED

    def on_eof(self):
        if isinstance(self._when_eof, Exception):
            raise self._when_eof
        return self._when_eof

    def with_records(self, records: List[Tuple[Any, Any]]) -> 'PyConnectTestSource':
        """
        Used to set this PyConnectTestSource's records to `records`.

        Returns the connector so the method can be chained with others.
        :param records: The records which shall be produced by this source connector.
        :return: self
        """
        self.records = records
        return self

    def when_eof(self, return_value) -> 'PyConnectTestSource':
        """
        Sets fixed a return value for :meth:`pyconnect.pyconnectsource.PyConnectSource.on_eof`.
        If the value is an Exception, it will be raised.

        Returns the connector so the method can be chained with others.
        :param return_value: The value or exception to be returned
                             or raised by :meth:`pyconnect.pyconnectsource.PyConnectSource.on_eof`.
        :return: self
        """
        self._when_eof = return_value
        return self

    def with_committed_offset(self, offset: Any) -> 'PyConnectTestSource':
        """
        Used to overwrite the commited offset (i.e. the offset it will use to start reading from).

        Returns the connector so the method can be chained with others.
        :param offset: offset used to start reading
        :return: self
        """
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
    """
    Implementation of a PyConnectSink that buffers incoming messges into a list
    :attr:`test.utils.PyConnectTestSink.message_buffer` and appends them into another
    one :attr:`test.utils.PyConnectTestSink.flushed_messages` on flush.
    Flush happens after every :attr:`test.utils.PyConnectTestSink.flush_interval` messages (defaults to 5).

    Its default behaviour is to stop when all subscribed partitions hit EOF.
    """

    def __init__(self, sink_config) -> None:
        self.message_buffer: List[Tuple[Any, Any]] = []
        self.flushed_messages: List[Tuple[Any, Any]] = []
        self.flush_interval = 5
        super().__init__(sink_config)

    def on_message_received(self, msg: Message) -> None:
        print(f'Message received: {message_repr(msg)}')
        # noinspection PyArgumentList
        self.message_buffer.append((msg.key(), msg.value()))

    def _check_status(self) -> None:
        """
        Utility function that prints consumer group status to stdout
        """
        print('Kafka consumer group status:')
        subprocess.call([
            os.path.join(CLI_DIR, 'kafka-consumer-groups.sh'),
            '--bootstrap-server', self.config['bootstrap_servers'][0],
            '--describe', '--group', self.config['group_id'],
            '--offsets', '--verbose'
        ])

    def on_startup(self) -> None:
        super().on_startup()
        print('######## CONSUMER STARUP #########')
        print(f'Config: {self.config!r}')
        self._check_status()

    def need_flush(self) -> bool:
        return len(self.message_buffer) == self.flush_interval

    def on_flush(self) -> None:
        print('Flushing messages:')
        pprint(self.message_buffer)
        self.flushed_messages.extend(self.message_buffer)
        self.message_buffer.clear()

    def on_shutdown(self) -> None:
        super().on_shutdown()
        print('######## CONSUMER SHUTDOWN #########')
        self._check_status()
        print('----\nFlushed messages:')
        pprint(self.flushed_messages)

    def on_no_message_received(self) -> Optional[Status]:
        if self.eof_reached != {} and all(self.eof_reached.values()):
            return Status.STOPPED
        return None


def rand_text(textlen: int) -> str:
    """
    Create random combinations of `texteln` letters coming from :data:`string.ascii_uppercase`.

    :param textlen: Length of the string that shall be returned.
    :return: Random string.
    """
    return ''.join(random.choices(string.ascii_uppercase, k=textlen))


def message_repr(msg: Message) -> str:
    """
    Returns out the representation of a :class:`confluent_kafka.Message`

    :param msg: The message which shall be turned into a string.
    :return: String representation of the message.
    """
    return (
        f'Message(key={msg.key()!r}, value={msg.value()!r}, topic={msg.topic()!r}, '
        f'partition={msg.partition()!r}, offset={msg.offset()!r}, error={msg.error()!r})'
    )


@pytest.fixture(params=[Status.CRASHED, TestException()],
                ids=['Status_CRASHED', 'TestException'])
def failing_callback(request):
    """
    returns a :class:`unittest.mock.Mock` object that either returns :obj:`pyconnect.core.Status.CRASHED` or raises
    :exc:`test.utils.TestException`.

    :return: Callback mock resulting in Status.CRASHED.
    """
    return mock.Mock(side_effect=it.repeat(request.param))


@pytest.fixture(scope='module')
def cluster_hosts() -> Dict[str, str]:
    """
    Reads the docker-compose.yml in order to determine the host names and ports of the different services necessary
    for the kafka cluster.
    :return: A map from service to url.
    """
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
def topic(request, cluster_hosts) -> Tuple[str, int]:
    """
    Creates a kafka topic consisting of a random 5 character string and being partition into 1, 2 or 4 partitions.
    Then it yields the tuple (topic, n_partitions).

    Prints topic information before and after topic was used by a test.
    :return: Topic and number of partitions within it.
    """
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
def message_factory() -> Callable[..., Message]:
    """
    Creates a factory for mocked :class:`confluent_kafka.Message` object.
    """
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
def error_message_factory(message_factory) -> Callable[..., Message]:
    """
    Creates a factory for mockec :class:`confluent_kafka.Message` that return a :class:`confluent_kafka.KafkaError`
    when :meth:`confluent_kafka.Message.error` is called on them.
    """
    with mock.patch('test.utils.KafkaError', autospec=True):
        def error_message_factory_(error_code=None):
            error = KafkaError()
            error.code.return_value = error_code
            return message_factory(error=error)

        yield error_message_factory_


@pytest.fixture
def eof_message(error_message_factory) -> Message:
    """
    Returns an EOF message.
    I.e. a :class:`confluent_kafka.Message` that holds a :class:`confluent_kafka.KafkaError` with error code
    :const:`confluent_kafka.KafkaError._PARTITION_EOF`.
    """
    return error_message_factory(error_code=KafkaError._PARTITION_EOF)
