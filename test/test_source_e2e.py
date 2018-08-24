from typing import Any, List, Tuple
from unittest import mock
from time import sleep

from confluent_kafka.avro import AvroConsumer
from confluent_kafka import KafkaError

import pytest
import itertools as it

from pyconnect.config import SourceConfig
from pyconnect.pyconnectsource import PyConnectSource
from pyconnect.core import Status

from test.utils import ConnectTestMixin

# noinspection PyUnresolvedReferences
from test.utils import cluster_hosts, topic


class PyConnectTestSource(ConnectTestMixin, PyConnectSource):

    def __init__(self, config: SourceConfig, records) -> None:
        super().__init__(config)
        self.records: List[Any] = records
        self.initial_idx = 0
        self.idx = 0

    def seek(self, idx: int):
        self.idx = idx

    def read(self) -> Any:
        try:
            record = self.records[self.idx]
        except IndexError:
            raise StopIteration()
        self.idx = self.get_next_index()
        return record

    def _get_committed_offset(self):
        return self.initial_idx

    def get_next_index(self):
        return self.idx + 1


@pytest.fixture
def source_factory(topic, cluster_hosts):
    topic_id, _ = topic

    config = SourceConfig(
        bootstrap_servers=cluster_hosts['broker'],
        schema_registry=cluster_hosts['schema-registry'],
        offset_topic=f'{topic_id}_offsets',
        flush_interval=5,
        topic=topic_id
    )

    def source_factory_(records: List[Tuple[Any, Any]], when_eof: Status):
        source = PyConnectTestSource(config, [])
        source.on_eof = mock.Mock(side_effect=it.repeat(when_eof))
        source.records = records
        return source

    yield source_factory_

@pytest.fixture
def consume_all(topic, cluster_hosts):
    topic_id, _ = topic

    consumer = AvroConsumer({
        'bootstrap.servers': cluster_hosts['broker'],
        'schema.registry.url':  cluster_hosts['schema-registry'],
        'group.id': f'{topic_id}_consumer',
        'enable.partition.eof': False
    })
    consumer.subscribe([topic_id])

    def consume_all_():
        messages = []
        while True:
            msg = consumer.poll(timeout=10)
            if msg is None:
                break
            if msg.error() is not None:
                assert msg.error().code() == KafkaError._PARTITION_EOF
                break
            messages.append(msg)
        return messages

    yield consume_all_
    consumer.close()


@pytest.fixture
def records():
    return [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5)
    ]


@pytest.mark.e2e
def test_produce_messages(source_factory, records, consume_all):
    source = source_factory(records=records, when_eof=Status.STOPPED)

    source.run()
    source._producer.flush()
    sleep(1)
    messages = consume_all()
    consumed_records = [(msg.key(), msg.value()) for msg in messages]

    assert set(records) == set(consumed_records)


