from unittest import mock
import pytest
from typing import List, Any

from pyconnect.config import SourceConfig
from pyconnect.pyconnectsource import PyConnectSource
from pyconnect.core import Status

from test.utils import ConnectTestMixin


class PyConnectTestSource(PyConnectSource, ConnectTestMixin):

    def __init__(self, config: SourceConfig, records) -> None:
        super().__init__(config)
        self.records: List[Any] = records
        self.idx = 0

    def seek(self, idx: int):
        self.idx = idx

    def read(self, timeout: int) -> Any:
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

    def source_factory_():
        source = PyConnectTestSource(config, [])
        source.on_eof = mock.Mock(return_value=Status.STOPPED)
    return source_factory_


def test_on_eof_called(source_factory):
    source = source_factory()

    source.run()

    source.on_eof.assert_called_once()


def test_message_sent(source_factory):
    source = source_factory()
    source.records = range(100)

    source.run()

    source.on_eof.assert_has_calls(map(mock.call, source.records))
