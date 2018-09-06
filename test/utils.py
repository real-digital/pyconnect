"""
This module contains utility functions and classes for testing.
"""
import pathlib
import random
import string
import subprocess
from pprint import pprint
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest
from confluent_kafka.cimpl import Message

from pyconnect.config import SourceConfig
from pyconnect.core import Status, message_repr
from pyconnect.pyconnectsink import PyConnectSink
from pyconnect.pyconnectsource import PyConnectSource

TEST_DIR = pathlib.Path(__file__).parent.absolute()
ROOT_DIR = TEST_DIR.parent
CLI_DIR = TEST_DIR / 'kafka' / 'bin'


class TestException(Exception):
    """
    Special exception used to make sure that it really IS the exception we raised that is propagated.

    Consider mocking a method with `side_effect=Exception()` vs `side_effect=TestException()`.
    You'd need `with pytest.raises(Exception):` vs. `with pytest.raises(TestException):`, where the former would pass
    even for TypeErr or ValueError that are not necessarily caused by the mock.
    """
    __test__ = False


# noinspection PyAttributeOutsideInit
class ConnectTestMixin:
    """
    Mixin class meant to stand between connector's abstract class and its implementing class. It will intercept
    or wrap some methods so we can cause certain behaviour and can make sure not to end in an endless loop.
    It will also add some utility methods so the desired behaviour is more easily configurable.
    """

    def __init__(self, conf: Dict[str, Any]) -> None:
        super().__init__(conf)
        self.forced_status_after_run: Optional[Status] = None
        self.run_counter = 0
        self.max_runs = 20
        self._ignore_crash = False

    def _run_loop(self) -> None:
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

    def on_crashed(self) -> Optional[Status]:
        try:
            new_status = super().on_crashed()
        except Exception as e:
            new_status = e

        if hasattr(self, '_when_crashed'):
            new_status = self._when_crashed

        if isinstance(new_status, Exception):
            raise new_status
        return new_status

    def on_shutdown(self) -> Optional[Status]:
        if self.status == Status.CRASHED and self._ignore_crash:
            self._status = Status.STOPPED
        return super().on_shutdown()

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

    def on_eof(self) -> Optional[Status]:
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

    def _get_committed_offset(self) -> Any:
        if hasattr(self, '_committed_offset'):
            return self._committed_offset
        return super()._get_committed_offset()

    def seek(self, idx: int) -> None:
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

    def get_index(self) -> Any:
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
            CLI_DIR / 'kafka-consumer-groups.sh',
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
