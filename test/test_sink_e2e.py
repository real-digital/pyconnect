from functools import partial
import os
import random
import subprocess
from typing import Callable, List, Tuple

from confluent_kafka import avro as confluent_avro
import pytest

from pyconnect.avroparser import to_key_schema, to_value_schema
from pyconnect.config import SinkConfig
from test.utils import CLI_DIR, PyConnectTestSink, TestException, rand_text
# noinspection PyUnresolvedReferences
from test.utils import cluster_hosts, topic

ConnectSinkFactory = Callable[..., PyConnectTestSink]


@pytest.fixture
def connect_sink_factory(cluster_hosts, topic) -> ConnectSinkFactory:
    """
    Creates a factory, that can be used to create readily usable instances of :class:`test.utils.PyConnectTestSink`.
    If necessary, any config parameter can be overwritten by providing a custom config as argument to the factory.
    """
    topic_id, partitions = topic
    group_id = topic_id + '_sink_group_id'
    sink_config = SinkConfig({
        'bootstrap_servers': cluster_hosts['broker'],
        'schema_registry': cluster_hosts['schema-registry'],
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


@pytest.fixture
def plain_avro_producer(cluster_hosts, topic) -> confluent_avro.AvroProducer:
    """
    Creates a plain `confluent_kafka.avro.AvroProducer` that can be used to publish messages.
    """
    topic_id, partitions = topic
    producer_config = {
        'bootstrap.servers': cluster_hosts['broker'],
        'schema.registry.url': cluster_hosts['schema-registry'],
        'group.id': topic_id + '_plain_producer_group_id'  # no idea what this does...
    }
    producer = confluent_avro.AvroProducer(producer_config)
    producer.produce = partial(producer.produce, topic=topic_id)

    return producer


@pytest.fixture
def produced_messages(plain_avro_producer, topic, cluster_hosts) -> List[Tuple[str, dict]]:
    """
    Creates 15 random messages, produces them to the currently active topic and then yields them for the test.
    """
    topic_id, partitions = topic
    messages = [
            (rand_text(8), {'a': rand_text(64), 'b': random.randint(0, 1000)})
            for _ in range(15)
    ]
    key, value = messages[0]
    key_schema = to_key_schema(key)
    value_schema = to_value_schema(value)

    for key, value in messages:
        plain_avro_producer.produce(
            key=key, value=value,
            key_schema=key_schema, value_schema=value_schema)

    plain_avro_producer.flush()

    result = subprocess.run([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--describe', '--topic', topic_id
    ], stderr=subprocess.PIPE, stdout=subprocess.PIPE)

    print(result.stdout.decode('utf-8'))
    if (result.stdout is None) or \
            (not len(result.stdout.splitlines()) == partitions+1):
        pytest.fail('not all partitions present!')

    yield messages


@pytest.mark.e2e
def test_message_consumption(produced_messages, connect_sink_factory: ConnectSinkFactory):
    connect_sink = connect_sink_factory()

    connect_sink.run()

    assert len(produced_messages) == len(connect_sink.flushed_messages)

    for message in produced_messages:
        assert message in connect_sink.flushed_messages


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

    for message in produced_messages:
        assert message in flushed_messages
