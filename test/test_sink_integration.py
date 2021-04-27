import functools
import threading
from typing import Callable, Dict, List, Tuple
from unittest import mock

import pytest
from loguru import logger

from pyconnect.config import SinkConfig
from pyconnect.core import Status

from .utils import PyConnectTestSink, TestException, compare_lists_unordered

ConnectSinkFactory = Callable[..., PyConnectTestSink]


@pytest.fixture
def connect_sink_factory(
    running_cluster_config: Dict[str, str], topic_and_partitions: Tuple[str, int]
) -> ConnectSinkFactory:
    """
    Creates a factory, that can be used to create readily usable instances of :class:`test.utils.PyConnectTestSink`.
    If necessary, any config parameter can be overwritten by providing a custom config as argument to the factory.
    """
    topic_id, _ = topic_and_partitions
    group_id = topic_id + "_sink_group_id"
    sink_config = SinkConfig(
        {
            "bootstrap_servers": running_cluster_config["broker"],
            "schema_registry": running_cluster_config["schema-registry"],
            "offset_commit_interval": 1,
            "group_id": group_id,
            "poll_timeout": 10.0,
            "topics": topic_id,
            "kafka_opts": {
                "allow.auto.create.topics": True,
                "auto.offset.reset": "earliest",
                "group.initial.rebalance.delay.ms": 100,
                "max.poll.records": 1,
                "default.topic.config": {"auto.offset.reset": "earliest"},
            },
            "unify_logging": True,
        }
    )

    def connect_sink_factory_(custom_config=None):
        if custom_config is not None:
            config = sink_config.copy()
            config.update(custom_config)
        else:
            config = sink_config
        test_sink = PyConnectTestSink(config)
        test_sink.max_runs = 30
        return test_sink

    return connect_sink_factory_


@pytest.mark.integration
def test_message_consumption(produced_messages: List[Tuple[str, dict]], connect_sink_factory: ConnectSinkFactory):
    connect_sink = connect_sink_factory()

    connect_sink.run()

    compare_lists_unordered(produced_messages, connect_sink.flushed_messages)


@pytest.mark.integration
def test_offset_commit_on_restart(produced_messages: List[Tuple[str, dict]], connect_sink_factory: ConnectSinkFactory):
    def patch_commit(sink: PyConnectTestSink) -> mock.Mock:
        old_func = sink._consumer.commit
        mocked_func = mock.Mock(name="commit", wraps=old_func)
        sink._consumer.commit = mocked_func
        return mocked_func

    connect_sink = connect_sink_factory()
    commit_mock = patch_commit(connect_sink)
    connect_sink.run()

    expected_call = commit_mock.call_args

    compare_lists_unordered(produced_messages, connect_sink.flushed_messages)

    connect_sink = connect_sink_factory()
    commit_mock = patch_commit(connect_sink)
    connect_sink.max_idle_count = 2
    connect_sink.run()

    assert len(expected_call[1]["offsets"]) > 0, f"No offsets commited during commit! {expected_call}"
    compare_lists_unordered(expected_call[1]["offsets"], commit_mock.call_args[1]["offsets"])


@pytest.mark.integration
def test_continue_after_crash(produced_messages: List[Tuple[str, dict]], connect_sink_factory: ConnectSinkFactory):
    connect_sink = connect_sink_factory({"kafka_opts": {"max.poll.interval.ms": 10000, "session.timeout.ms": 6000}})
    connect_sink.with_method_raising_after_n_calls("on_message_received", TestException(), 7)
    connect_sink.with_mock_for("close")

    with pytest.raises(TestException):
        connect_sink.run()
    flushed_messages = connect_sink.flushed_messages

    connect_sink = connect_sink_factory()

    connect_sink.run()

    flushed_messages.extend(connect_sink.flushed_messages)

    compare_lists_unordered(produced_messages, flushed_messages)


def use_barrier(barrier: threading.Barrier, sink: PyConnectTestSink, callback_name: str) -> Callable:
    callback = getattr(sink, callback_name)

    @functools.wraps(callback)
    def wrapper(*args, **kwargs):
        if not barrier.broken:
            for _ in range(12):
                try:
                    logger.info("Waiting for barrier.")
                    i = barrier.wait(timeout=10)
                    logger.info("Barrier reached.")
                    if i == 0:
                        logger.info("Deactivating barrier.")
                        barrier.abort()
                except TimeoutError:
                    logger.info("Barrier still blocked, call consume(0) to allow for rebalancing.")
                    sink._consumer.poll(0)
                except threading.BrokenBarrierError:
                    # Can only happen when the barrier is passed, because that's the only time when "abort" is called.
                    logger.info("Got BrokenBarrierError, that's fine.")
                break
            else:
                raise TimeoutError("Timeout waiting for barrier")
        return callback(*args, **kwargs)

    return wrapper


@pytest.mark.integration
def test_two_sinks_one_failing(
    topic_and_partitions: Tuple[str, int], produced_messages: List[Tuple[str, dict]], connect_sink_factory
):
    _, partitions = topic_and_partitions
    if partitions == 1:
        return  # we need to test multiple consumers on multiple partitions for rebalancing issues
    conf = {"offset_commit_interval": 2}

    barrier = threading.Barrier(2, timeout=120)

    failing_sink: PyConnectTestSink = connect_sink_factory(conf)
    failing_sink.with_method_raising_after_n_calls("on_message_received", TestException(), 3)
    failing_sink.on_message_received = use_barrier(barrier, failing_sink, "on_message_received")
    failing_sink.with_wrapper_for("on_message_received")

    running_sink = connect_sink_factory(conf)
    running_sink.on_message_received = use_barrier(barrier, running_sink, "on_message_received")
    running_sink.with_wrapper_for("on_message_received")
    running_sink.max_idle_count = 5

    running_sink_thread = threading.Thread(target=running_sink.run, name="RUNNING Sink")
    failing_sink_thread = threading.Thread(target=failing_sink.run, name="FAILING Sink")

    running_sink_thread.start()
    failing_sink_thread.start()

    running_sink_thread.join()
    failing_sink_thread.join()

    assert running_sink.on_message_received.called, "Running sink should have received messages"
    assert failing_sink.on_message_received.called, "Failing sink should have received messages"
    assert len(failing_sink.flushed_messages) == 2, "Only messages before crash should be flushed"
    assert failing_sink.status == Status.CRASHED
    assert isinstance(failing_sink.status_info, TestException)
    assert running_sink.status == Status.STOPPED

    flushed_messages = running_sink.flushed_messages + failing_sink.flushed_messages
    compare_lists_unordered(produced_messages, flushed_messages)
