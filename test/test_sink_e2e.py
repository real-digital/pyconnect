import threading
from typing import Callable

import pytest

from pyconnect.config import SinkConfig
from pyconnect.core import Status
from .utils import PyConnectTestSink, TestException, compare_lists_unordered

ConnectSinkFactory = Callable[..., PyConnectTestSink]


@pytest.fixture
def connect_sink_factory(running_cluster_config, topic) -> ConnectSinkFactory:
    """
    Creates a factory, that can be used to create readily usable instances of :class:`test.utils.PyConnectTestSink`.
    If necessary, any config parameter can be overwritten by providing a custom config as argument to the factory.
    """
    topic_id, partitions = topic
    group_id = topic_id + '_sink_group_id'
    sink_config = SinkConfig({
        'bootstrap_servers': running_cluster_config['broker'],
        'schema_registry': running_cluster_config['schema-registry'],
        'offset_commit_interval': 1,
        'group_id': group_id,
        'poll_timeout': 10,
        'topics': topic_id
    })

    def connect_sink_factory_(custom_config=None):
        if custom_config is not None:
            config = sink_config.copy()
            config.update(custom_config)
        else:
            config = sink_config
        return PyConnectTestSink(config)
    return connect_sink_factory_


@pytest.mark.e2e
def test_message_consumption(produced_messages, connect_sink_factory: ConnectSinkFactory):
    connect_sink = connect_sink_factory()

    connect_sink.run()

    compare_lists_unordered(produced_messages, connect_sink.flushed_messages)


@pytest.mark.e2e
def test_continue_after_crash(produced_messages, connect_sink_factory: ConnectSinkFactory):
    connect_sink = connect_sink_factory()
    connect_sink.with_method_raising_after_n_calls('on_message_received', TestException(), 7)
    connect_sink.with_mock_for('close')

    with pytest.raises(TestException):
        connect_sink.run()
    flushed_messages = connect_sink.flushed_messages

    connect_sink = connect_sink_factory()

    connect_sink.run()

    flushed_messages.extend(connect_sink.flushed_messages)

    compare_lists_unordered(produced_messages, flushed_messages)


@pytest.mark.e2e
def test_two_sinks_one_failing(topic, produced_messages, connect_sink_factory):
    _, partitions = topic
    if partitions == 1:
        return  # we need to test multiple consumers on multiple partitions for rebalancing issues
    conf = {
        'offset_commit_interval': 2,
    }

    failing_sink = connect_sink_factory(conf)
    failing_sink.with_method_raising_after_n_calls('on_message_received', TestException(), 3)
    failing_sink.with_wrapper_for('on_message_received')

    running_sink = connect_sink_factory(conf)
    running_sink.with_wrapper_for('on_message_received')
    running_sink.max_idle_count = 5

    running_sink_thread = threading.Thread(target=running_sink.run, name='RUNNING Sink')
    failing_sink_thread = threading.Thread(target=failing_sink.run, name='FAILING Sink')

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
