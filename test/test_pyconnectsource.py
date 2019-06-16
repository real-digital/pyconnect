from typing import Callable, cast
from unittest import mock

import pytest

from pyconnect.config import SourceConfig
from pyconnect.core import NoCrashInfo
from .utils import PyConnectTestSource, TestException

SourceFactory = Callable[..., PyConnectTestSource]


@pytest.fixture
def source_factory(message_factory):
    """
    Creates a factory, that can be used to create readily usable instances of :class:`test.utils.PyConnectTestSource`.
    """
    config = SourceConfig(
        dict(
            bootstrap_servers="testserver",
            offset_topic="testtopic",
            schema_registry="testregistry",
            offset_commit_interval=5,
            topic="testtopic",
        )
    )

    with mock.patch("pyconnect.pyconnectsource.AvroProducer"), mock.patch("pyconnect.pyconnectsource.AvroConsumer"):

        def source_factory_():
            source = PyConnectTestSource(config).with_committed_offset(0)

            return source

        yield source_factory_


def test_on_eof_called(source_factory: SourceFactory):
    # standard setup: no records, will call eof immediately which in turn stops producer
    source = source_factory().with_wrapper_for("on_eof")

    source.run()

    cast(mock.Mock, source.on_eof).assert_called_once()


def test_message_sent(source_factory: SourceFactory):
    records = [(1, 1), (2, 2), (3, 3)]
    source = source_factory().with_records(records).with_wrapper_for("_produce")

    source.run()
    calls = [mock.call(key, value) for key, value in records]
    cast(mock.Mock, source._produce).assert_has_calls(calls)


def test_seek_is_called(source_factory: SourceFactory):
    source = source_factory().with_wrapper_for("seek")

    source.run()

    cast(mock.Mock, source.seek).assert_called_once()


def test_seek_not_called_when_no_committed_offset(source_factory: SourceFactory):
    source = source_factory().with_committed_offset(None).with_wrapper_for("seek")

    source.run()

    assert not cast(mock.Mock, source.seek).called


def test_no_run_if_seek_fails(source_factory: SourceFactory, failing_callback: mock.Mock):
    source = source_factory().with_wrapper_for("_run_once")
    source.seek = failing_callback

    try:
        source.run()
    except TestException:
        pass
    except NoCrashInfo:
        pass
    else:
        pytest.fail("No Exception raised!")

    assert not cast(mock.Mock, source._run_once).called
    assert source.seek.called


def test_committed_offset_is_used(source_factory: SourceFactory):
    records = [(0, 0), (1, 1), (2, 2), (3, 3)]
    source = source_factory().with_records(records).with_committed_offset(2).with_mock_for("_produce")

    source.run()

    calls = [mock.call(key, value) for key, value in records[2:]]
    cast(mock.Mock, source._produce).assert_has_calls(calls)


def test_exception_raised_on_shutdown(source_factory: SourceFactory):
    source = source_factory().when_eof(TestException())

    with pytest.raises(TestException):
        source.run()


def test_commit_is_called(source_factory: SourceFactory):
    source = source_factory().with_mock_for("_commit")
    source.run()
    assert cast(mock.Mock, source._commit).called
