from typing import Callable

import pytest

from pyconnect.config import SinkConfig
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
