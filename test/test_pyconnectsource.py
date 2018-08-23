from unittest import mock
import pytest
from typing import List, Any

from pyconnect.config import SourceConfig
from pyconnect.pyconnectsource import PyConnectSource
from pyconnect.core import Status

from test.utils import ConnectTestMixin


class PyConnectTestSource(ConnectTestMixin, PyConnectSource):

    def __init__(self, config: SourceConfig, records) -> None:
        super().__init__(config)
        self.records: List[Any] = records
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

    def get_next_index(self):
        return self.idx + 1


@pytest.fixture
def source_factory():
    config = SourceConfig(
        bootstrap_servers='testserver',
        offset_topic='testtopic',
        schema_registry='testregistry',
        flush_interval=5
    )

    with mock.patch('pyconnect.pyconnectsource.AvroProducer'):
        def source_factory_():
            source = PyConnectTestSource(config, [])
            source.on_eof = mock.Mock(return_value=Status.STOPPED)
            return source
        yield source_factory_


def test_on_eof_called(source_factory):
    source = source_factory()

    source.run()

    source.on_eof.assert_called_once()


def test_message_sent(source_factory):
    source = source_factory()
    source.records = list(zip(range(10), range(10)))

    source.run()
    calls = [mock.call(key=key, value=value) for key, value in source.records]
    source._producer.produce.assert_has_calls(calls)
