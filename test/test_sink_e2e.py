from functools import partial
from unittest import mock
from confluent_kafka import avro as confluent_avro
import subprocess
import pytest
import os
import random

from pyconnect.config import SinkConfig
from pyconnect.pyconnectsink import Status

from test.utils import PyConnectTestSink, rand_text, to_schema, CLI_DIR

# noinspection PyUnresolvedReferences
from test.utils import cluster_hosts, topic


@pytest.fixture
def sink_config(cluster_hosts, topic):
    topic_id, partitions = topic
    group_id = topic_id + '_sink_group_id'
    config = SinkConfig(
            bootstrap_servers=cluster_hosts['broker'],
            schema_registry=cluster_hosts['schema-registry'],
            flush_interval=1,
            group_id=group_id,
            poll_timeout=2,
            topics=topic_id
    )
    yield config


@pytest.fixture
def connect_sink_factory(sink_config):
    def connect_sink_factory_():
        return PyConnectTestSink(sink_config)
    return connect_sink_factory_


@pytest.fixture
def plain_avro_producer(cluster_hosts, topic):
    topic_id, partitions = topic
    producer_config = {
        'bootstrap.servers': cluster_hosts['broker'],
        'schema.registry.url': cluster_hosts['schema-registry'],
        'group.id': topic_id + '_plain_producer_group_id'
    }
    producer = confluent_avro.AvroProducer(producer_config)
    producer.produce = partial(producer.produce, topic=topic_id)

    return producer


@pytest.fixture
def produced_messages(plain_avro_producer, topic, cluster_hosts):
    topic_id, partitions = topic
    messages = [
            (rand_text(8), {'a': rand_text(64), 'b': random.randint(0, 1000)})
            for _ in range(15)
    ]
    key_schema = to_schema('key', messages[0][0])
    value_schema = to_schema('value', messages[0][1])

    for key, value in messages:
        plain_avro_producer.produce(
            key=key, value=value,
            key_schema=key_schema, value_schema=value_schema)

    plain_avro_producer.flush()

    result = subprocess.run([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--describe', '--topic', topic_id
    ], capture_output=True)

    print(result.stdout.decode('utf-8'))
    if (result.stdout is None) or \
            (not len(result.stdout.splitlines()) == partitions+1):
        pytest.fail('not all partitions present!')

    yield messages


@pytest.mark.e2e
def test_message_consumption(produced_messages, connect_sink_factory):
    connect_sink = connect_sink_factory()
    # stop after 2 empty messages were received
    connect_sink.on_no_message_received = mock.Mock(
        side_effect=[None]*0 + [Status.STOPPED])

    connect_sink.run()

    assert len(produced_messages) == len(connect_sink.flushed_messages)

    for message in produced_messages:
        assert message in connect_sink.flushed_messages


@pytest.mark.e2e
def test_continue_after_crash(produced_messages, connect_sink_factory):
    connect_sink = connect_sink_factory()
    connect_sink.forced_status_after_run = [None]*7 + [Status.CRASHED]

    connect_sink.run()
    flushed_messages = connect_sink.flushed_messages

    connect_sink = connect_sink_factory()
    # it takes a while until partition assignment is complete and messages
    # start arriving
    # TODO: see if consumer.assignment() is an indicator for this
    # maybe we can use on_assign and on_revoke to figure out whether to poll
    # or to wait
    connect_sink.on_no_message_received = mock.Mock(
        side_effect=[None]*5 + [Status.STOPPED])

    connect_sink.run()

    flushed_messages.extend(connect_sink.flushed_messages)

    for message in produced_messages:
        assert message in flushed_messages
