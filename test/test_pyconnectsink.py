from typing import Callable, cast
from unittest import mock

import pytest

from pyconnect.config import SinkConfig
from pyconnect.core import NoCrashInfo
from pyconnect.pyconnectsink import Status
from .utils import PyConnectTestSink, TestException

SinkFactory = Callable[..., PyConnectTestSink]


@pytest.fixture
def sink_factory():
    conf = SinkConfig(
        dict(
            bootstrap_servers="localhost",
            schema_registry="localhost",
            offset_commit_interval=1,
            group_id="group_id",
            poll_timeout=1,
            topics="",
        )
    )
    with mock.patch("pyconnect.pyconnectsink.RichAvroConsumer", autospec=True):

        def sink_factory_():
            sink = PyConnectTestSink(conf)
            sink._consumer.poll.return_value = None
            sink._check_status = mock.Mock()
            return sink

        yield sink_factory_


@pytest.fixture
def run_once_sink(sink_factory: SinkFactory) -> PyConnectTestSink:
    sink = sink_factory()
    sink.forced_status_after_run = Status.STOPPED

    return sink


def test_callbacks_are_called(sink_factory: SinkFactory, message_factory, error_message_factory) -> None:
    # setup
    connect_sink = sink_factory()
    patcher = mock.patch.multiple(
        connect_sink,
        on_message_received=mock.Mock(return_value=None),
        on_no_message_received=mock.Mock(return_value=None),
        need_flush=mock.Mock(return_value=True),
        on_flush=mock.Mock(return_value=None),
        on_error_received=mock.Mock(return_value=None),
        on_crash_during_run=mock.Mock(return_value=None),
        on_startup=mock.Mock(return_value=None),
        on_shutdown=mock.Mock(return_value=None),
    )
    patcher.start()

    msg = message_factory()
    error_msg = error_message_factory()

    # return normal_message
    # then empty message
    # then error message
    # then raise exception
    connect_sink._consumer.poll.side_effect = [msg, None, error_msg, TestException()]

    # perform
    with pytest.raises(TestException):
        connect_sink.run()

    # test
    assert cast(mock.Mock, connect_sink.on_startup).called

    assert cast(mock.Mock, connect_sink.on_message_received).called_with(msg)
    assert cast(mock.Mock, connect_sink.need_flush).called
    assert cast(mock.Mock, connect_sink.on_flush).called
    assert cast(mock.Mock, connect_sink.on_no_message_received).called
    assert cast(mock.Mock, connect_sink.on_error_received).called_with(error_msg)
    assert cast(mock.Mock, connect_sink.on_crash_during_run).called

    assert cast(mock.Mock, connect_sink.on_shutdown).called


def test_no_commit_if_flush_failed(run_once_sink: PyConnectTestSink, failing_callback: mock.Mock):
    # setup
    run_once_sink.need_flush = mock.Mock(return_value=True)
    run_once_sink.on_flush = failing_callback

    # perform
    try:
        run_once_sink.run()
    except TestException:
        pass
    except NoCrashInfo:
        pass
    else:
        pytest.fail("No Exception raised!")

    # test
    assert cast(mock.Mock, run_once_sink.on_flush).called
    assert not cast(mock.Mock, run_once_sink._consumer.commit).called


def test_commit_after_flush(message_factory, run_once_sink: PyConnectTestSink):
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


def test_current_message_is_set(message_factory, run_once_sink: PyConnectTestSink):
    # setup
    msg = message_factory()
    run_once_sink._consumer.poll.return_value = msg

    # perform
    run_once_sink.run()

    # test
    assert run_once_sink.current_message is msg


def test_current_message_is_unset(sink_factory: SinkFactory, message_factory):
    # setup
    msg = message_factory()
    connect_sink = sink_factory()
    connect_sink._consumer.poll.side_effect = [msg, TestException()]

    # perform
    with pytest.raises(TestException):
        connect_sink.run()

    # test
    assert connect_sink.current_message is None


def test_status_info_is_set(sink_factory: SinkFactory, message_factory):
    # setup
    msg = message_factory()
    exception = TestException()
    connect_sink = sink_factory()
    connect_sink._consumer.poll.side_effect = [msg, exception]

    # perform
    with pytest.raises(TestException):
        connect_sink.run()

    # test
    assert connect_sink.status_info is exception


def test_status_info_is_unset(sink_factory: SinkFactory):
    # setup
    connect_sink = sink_factory()
    connect_sink._consumer.poll.side_effect = [TestException(), None]
    connect_sink.on_crash_during_run = mock.Mock(return_value=Status.RUNNING)
    connect_sink.forced_status_after_run = [None, Status.STOPPED]

    # perform
    connect_sink.run()

    # test
    assert connect_sink.status_info is None


def test_crash_handler_to_the_rescue(sink_factory: SinkFactory, message_factory, failing_callback):
    # Client libraries can implement a crash handling mechanism that allows the
    # consumer to recover from an exception.

    # setup
    msg1 = message_factory()
    msg2 = message_factory()

    connect_sink = sink_factory()
    connect_sink._consumer.poll.side_effect = [msg1, None, msg2]
    connect_sink.on_no_message_received = failing_callback

    connect_sink.on_crash_during_run = mock.Mock(return_value=Status.RUNNING)
    connect_sink.on_message_received = mock.Mock(side_effect=[None, Status.STOPPED])

    # perform
    connect_sink.run()

    # test
    connect_sink.on_crash_during_run.assert_called_once()
    connect_sink.on_message_received.assert_has_calls([mock.call(msg1), mock.call(msg2)])
    assert connect_sink._status == Status.STOPPED


def test_flush_if_needed(run_once_sink: PyConnectTestSink):
    run_once_sink.need_flush = mock.Mock(return_value=True)

    # make sure on_shutdown doesn't call on_flush
    run_once_sink.on_shutdown = mock.Mock(return_value=None)
    run_once_sink.on_flush = mock.Mock(return_value=None)

    run_once_sink.run()

    run_once_sink.on_flush.assert_called_once()


def test_no_flush_if_not_needed(run_once_sink: PyConnectTestSink):
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # make sure on_shutdown doesn't call on_flush
    run_once_sink.on_shutdown = mock.Mock(return_value=None)
    run_once_sink.on_flush = mock.Mock(return_value=None)

    run_once_sink.run()

    assert not run_once_sink.on_flush.called


def test_final_flush_called(run_once_sink: PyConnectTestSink):
    # setup
    run_once_sink.on_flush = mock.Mock(return_value=None)
    # should be called even if flush_needed returns False
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # perform
    run_once_sink.run()

    # test
    run_once_sink.on_flush.assert_called_once()


def test_no_commit_if_final_flush_failed(run_once_sink: PyConnectTestSink, failing_callback: mock.Mock):
    # setup
    run_once_sink.on_flush = failing_callback
    # make sure standard flush is not called
    run_once_sink.need_flush = mock.Mock(return_value=False)

    # perform
    try:
        run_once_sink.run()
    except TestException:
        pass
    except NoCrashInfo:
        pass
    else:
        pytest.fail("No Exception raised!")

    # test
    run_once_sink.on_flush.assert_called_once()
    assert not cast(mock.Mock, run_once_sink._consumer.commit).called


def test_flush_after_run(sink_factory: SinkFactory, message_factory):
    # setup
    connect_sink = sink_factory()
    connect_sink._consumer.poll.side_effect = [message_factory()] * 5 + [None]
    connect_sink.on_no_message_received = mock.Mock(return_value=Status.STOPPED)
    connect_sink.on_flush = mock.Mock(return_value=None)
    connect_sink.need_flush = mock.Mock(return_value=False)

    # perform
    connect_sink.run()

    # test
    connect_sink._consumer.commit.assert_called_once()
    connect_sink.on_flush.assert_called_once()


def test_no_msg_handling_after_failed_flush(sink_factory: SinkFactory, failing_callback: mock.Mock, message_factory):
    connect_sink = sink_factory()
    connect_sink.on_flush = failing_callback
    connect_sink._consumer.poll.side_effect = [message_factory()] * 5 + [None]
    connect_sink.on_no_message_received = mock.Mock(return_value=Status.STOPPED)

    connect_sink.on_message_received = mock.Mock(return_value=None)
    connect_sink.need_flush = mock.Mock(return_value=True)

    try:
        connect_sink.run()
    except TestException:
        pass
    except NoCrashInfo:
        pass
    else:
        pytest.fail("No Exception raised!")

    assert not connect_sink.on_message_received.called
