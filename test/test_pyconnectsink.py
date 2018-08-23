from unittest import mock
import pytest

from pyconnect.pyconnectsink import Status
from pyconnect.pyconnectsink import PyConnectSink
from pyconnect.config import SinkConfig

from confluent_kafka import Message
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError

from test.utils import ConnectTestMixin


class PyConnectTestSink(ConnectTestMixin, PyConnectSink):

    def __init__(self) -> None:
        conf_dict = dict(
                bootstrap_servers='localhost',
                schema_registry='localhost',
                flush_interval=1,
                group_id='group_id',
                poll_timeout=1,
                topics=''
        )
        super().__init__(SinkConfig(**conf_dict))  # type: ignore

    def on_message_received(self, msg: Message) -> None:
        pass

    def on_flush(self) -> None:
        pass

    def _make_consumer(self):
        with mock.patch('test.test_pyconnectsink.AvroConsumer', autospec=True):
            consumer = AvroConsumer(None)
            consumer.poll.return_value = None
        return consumer


@pytest.fixture
def run_once_sink():
    sink = PyConnectTestSink()
    sink.forced_status_after_run = Status.STOPPED
    return sink


@pytest.fixture
def message_factory():
    with mock.patch('test.test_pyconnectsink.Message', autospec=True):
        def message_factory_():
            msg = Message()
            msg.error.return_value = None
            msg.topic.return_value = 'testtopic'
            msg.partition.return_value = 1
            msg.offset.return_value = 1
            return msg
        yield message_factory_


@pytest.fixture
def error_message_factory(message_factory):
    with mock.patch('test.test_pyconnectsink.KafkaError', autospec=True):
        def error_message_factory_():
            error = KafkaError()
            msg = message_factory()
            msg.error.return_value = error

        yield error_message_factory_


def test_callbacks_are_called(message_factory, error_message_factory) -> None:
    # setup
    connect_sink = mock.Mock(wraps=PyConnectTestSink)()
    connect_sink.on_message_received = mock.Mock(return_value=None)
    connect_sink.on_no_message_received = mock.Mock(return_value=None)
    connect_sink.need_flush = mock.Mock(return_value=True)
    connect_sink.on_flush = mock.Mock(return_value=None)
    connect_sink.on_error_received = mock.Mock(return_value=None)
    connect_sink.on_crash = mock.Mock(return_value=None)
    connect_sink.on_startup = mock.Mock(return_value=None)
    connect_sink.on_shutdown = mock.Mock(return_value=None)

    msg = message_factory()
    error_msg = error_message_factory()

    # return normal_message
    # then empty message
    # then error message
    # then raise exception
    connect_sink._consumer.poll.side_effect = [
            msg,
            None,
            error_msg,
            Exception()]

    # perform
    connect_sink.run()

    # test
    assert connect_sink.on_startup.called

    assert connect_sink.on_message_received.called_with(msg)
    assert connect_sink.need_flush.called
    assert connect_sink.on_flush.called
    assert connect_sink.on_no_message_received.called
    assert connect_sink.on_error_received.called_with(error_msg)
    assert connect_sink.on_crash.called

    assert connect_sink.on_shutdown.called


def test_no_commit_if_flush_failed_with_status(run_once_sink):
    # setup
    run_once_sink.need_flush = mock.Mock(return_value=True)
    run_once_sink.on_flush = mock.Mock(return_value=Status.CRASHED)

    # fail with status response

    # perform
    run_once_sink.run()

    # test
    assert run_once_sink.on_flush.called
    assert not run_once_sink._consumer.commit.called


def test_no_commit_if_flush_failed_with_exception(run_once_sink):
    # setup
    run_once_sink.need_flush = mock.Mock(return_value=True)
    run_once_sink.on_flush = mock.Mock(side_effect=Exception())

    # fail with exception

    # perform
    run_once_sink.run()

    # test
    assert run_once_sink.on_flush.called
    assert not run_once_sink._consumer.commit.called


def test_commit_after_flush(message_factory, run_once_sink):
    # setup
    msg = message_factory()
    run_once_sink.need_flush = mock.Mock(return_value=True)
    run_once_sink.on_flush = mock.Mock(return_value=None)
    run_once_sink._consumer.poll.return_value = msg

    # perform
    run_once_sink.run()

    # test
    assert run_once_sink.on_flush.called
    assert run_once_sink._consumer.commit.called


def test_last_message_is_set(message_factory, run_once_sink):
    # setup
    msg = message_factory()
    run_once_sink._consumer.poll.return_value = msg

    # perform
    run_once_sink.run()

    # test
    assert run_once_sink.last_message is msg


def test_last_message_is_unset(message_factory):
    # setup
    msg = message_factory()
    connect_sink = PyConnectTestSink()
    connect_sink._consumer.poll.side_effect = [msg, Exception()]

    # perform
    connect_sink.run()

    # test
    assert connect_sink.last_message is None


def test_status_info_is_set(message_factory):
    # setup
    msg = message_factory()
    exception = Exception()
    connect_sink = PyConnectTestSink()
    connect_sink._consumer.poll.side_effect = [msg, exception]

    # perform
    connect_sink.run()

    # test
    assert connect_sink.status_info is exception


def test_status_info_is_unset():
    # setup
    connect_sink = PyConnectTestSink()
    connect_sink._consumer.poll.side_effect = [Exception(), None]
    connect_sink.on_crash = mock.Mock(return_value=Status.RUNNING)
    connect_sink.forced_status_after_run = [None, Status.STOPPED]

    # perform
    connect_sink.run()

    # test
    assert connect_sink.status_info is None


def test_crash_handler_to_the_rescue(message_factory):
    # Client libraries can implement a crash handling mechanism that allows the
    # consumer to recover from an exception.

    # setup
    msg1 = message_factory()
    msg2 = message_factory()

    connect_sink = PyConnectTestSink()
    connect_sink._consumer.poll.side_effect = [msg1, Exception(), msg2]

    connect_sink.on_crash = mock.Mock(return_value=Status.RUNNING)
    connect_sink.on_message_received = mock.Mock(
        side_effect=[None, Status.STOPPED])

    # perform
    connect_sink.run()

    # test
    connect_sink.on_crash.assert_called_once()
    connect_sink.on_message_received.assert_has_calls([
        mock.call(msg1), mock.call(msg2)])
    assert connect_sink._status == Status.STOPPED


def test_flush_if_needed(run_once_sink):
    run_once_sink.need_flush = mock.Mock(return_value=True)

    # make sure on_final_flush doesn't call on_flush
    run_once_sink.on_final_flush = mock.Mock(return_value=None)
    run_once_sink.on_flush = mock.Mock(return_value=None)

    run_once_sink.run()

    run_once_sink.on_flush.assert_called_once()


def test_no_flush_if_not_needed(run_once_sink):
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # make sure on_final_flush doesn't call on_flush
    run_once_sink.on_final_flush = mock.Mock(return_value=None)
    run_once_sink.on_flush = mock.Mock(return_value=None)

    run_once_sink.run()

    assert not run_once_sink.on_flush.called


def test_on_final_flush_called(run_once_sink):
    # setup
    run_once_sink.on_final_flush = mock.Mock(return_value=None)
    # should be called even if flush_needed returns False
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # perform
    run_once_sink.run()

    # test
    run_once_sink.on_final_flush.assert_called_once()


def test_no_commit_if_final_flush_failed_with_status(run_once_sink):
    # setup
    run_once_sink.on_final_flush = mock.Mock(return_value=Status.CRASHED)
    # make sure standard flush is not called
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # perform
    run_once_sink.run()

    # test
    run_once_sink.on_final_flush.assert_called_once()
    assert not run_once_sink._consumer.commit.called


def test_no_commit_if_final_flush_failed_with_exception(run_once_sink):
    # setup
    run_once_sink.on_final_flush = mock.Mock(side_effect=[Exception()])
    # make sure standard flush is not called
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # perform
    run_once_sink.run()

    # test
    run_once_sink.on_final_flush.assert_called_once()
    assert not run_once_sink._consumer.commit.called


def test_flush_after_run(message_factory):
    # setup
    connect_sink = PyConnectTestSink()
    connect_sink._consumer.poll.side_effect = [message_factory()]*5 + [None]
    connect_sink.on_no_message_received = mock.Mock(
        return_value=Status.STOPPED)
    connect_sink.on_flush = mock.Mock(return_value=None)
    connect_sink.need_flush = mock.Mock(return_value=False)

    # perform
    connect_sink.run()

    # test
    connect_sink._consumer.commit.assert_called_once()
    connect_sink.on_flush.assert_called_once()
