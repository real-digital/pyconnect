from unittest import mock
import pytest

from pyconnect.config import SourceConfig
from pyconnect.core import Status

from test.utils import PyConnectTestSource, TestException

# noinspection PyUnresolvedReferences
from test.utils import failing_callback, eof_message, error_message_factory, message_factory


@pytest.fixture
def source_factory(eof_message, message_factory):
    config = SourceConfig(
        bootstrap_servers='testserver',
        offset_topic='testtopic',
        schema_registry='testregistry',
        flush_interval=5,
        topic='testtopic'
    )

    with mock.patch('pyconnect.pyconnectsource.AvroProducer'), \
            mock.patch('pyconnect.pyconnectsource.AvroConsumer'):

        def source_factory_():
            source = PyConnectTestSource(config).with_committed_offset(0)

            return source

        yield source_factory_


def test_on_eof_called(source_factory):
    # standard setup: no records, will call eof immediately which in turn stops producer
    source = source_factory().with_wrapper_for('on_eof')

    source.run()

    source.on_eof.assert_called_once()


def test_message_sent(source_factory):
    records = [(1, 1), (2, 2), (3, 3)]
    source = source_factory().with_records(records).with_wrapper_for('_produce')

    source.run()
    calls = [mock.call(key, value) for key, value in records]
    source._produce.assert_has_calls(calls)


def test_seek_is_called(source_factory):
    source = source_factory().with_wrapper_for('seek')

    source.run()

    source.seek.assert_called_once()


def test_seek_not_called_when_no_committed_offset(source_factory):
    source = source_factory().with_committed_offset(None).with_wrapper_for('seek')

    source.run()

    assert not source.seek.called


def test_no_run_if_seek_fails(source_factory, failing_callback):
    source = source_factory().with_wrapper_for('_run_once').ignoring_crash_on_shutdown()
    source.seek = failing_callback

    source.run()

    assert not source._run_once.called
    assert source.seek.called


def test_committed_offset_is_used(source_factory):
    records = [(0, 0), (1, 1), (2, 2), (3, 3)]
    source = source_factory().with_records(records).with_committed_offset(2).with_mock_for('_produce')

    source.run()

    calls = [mock.call(key, value) for key, value in records[2:]]
    source._produce.assert_has_calls(calls)


def test_exception_raised_on_shutdown(source_factory):
    source = source_factory().when_eof(TestException())

    with pytest.raises(TestException):
        source.run()

def test_commit_is_called(source_factory):
    source = source_factory().with_mock_for('_commit')
    source.run()
    assert source._commit.called