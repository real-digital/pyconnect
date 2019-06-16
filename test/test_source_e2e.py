from time import sleep
from typing import Callable, Iterable

import pytest

from pyconnect.config import SourceConfig
from test.conftest import ConsumeAll, RecordList
from .utils import PyConnectTestSource, compare_lists_unordered

SourceFactory = Callable[..., PyConnectTestSource]


@pytest.fixture
def source_factory(topic, running_cluster_config) -> Iterable[SourceFactory]:
    """
    Creates a factory, that can be used to create readily usable instances of :class:`test.utils.PyConnectTestSource`.
    """
    topic_id, _ = topic

    config = SourceConfig(
        dict(
            bootstrap_servers=running_cluster_config["broker"],
            schema_registry=running_cluster_config["schema-registry"],
            offset_topic=f"{topic_id}_offsets",
            offset_commit_interval=5,
            topic=topic_id,
        )
    )

    def source_factory_() -> PyConnectTestSource:
        source = PyConnectTestSource(config)
        return source

    yield source_factory_


@pytest.mark.e2e
def test_produce_messages(source_factory: SourceFactory, records: RecordList, consume_all: ConsumeAll):
    source = source_factory().with_records(records)

    source.run()
    source._producer.flush()
    sleep(1)
    consumed_records = consume_all()

    compare_lists_unordered(consumed_records, records)


@pytest.mark.e2e
def test_resume_producing(source_factory: SourceFactory, consume_all: ConsumeAll):
    first_records = [(1, 1), (2, 2), (3, 3)]
    first_source = source_factory().with_records(first_records)

    false_first_records = [(-1, -1), (-2, -2), (-3, -3)]
    second_records = [(4, 4), (5, 5), (6, 6)]
    second_source = source_factory().with_records(false_first_records + second_records)

    first_source.run()
    second_source.run()
    consumed_records = consume_all()

    assert set(consumed_records) == set(first_records + second_records)
