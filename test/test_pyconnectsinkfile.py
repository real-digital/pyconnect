import pytest

from pyconnect.pyconnectsink import Status
from test.pyconnectsinkfile import PyConnectSinkFile
from test.test_kafka_api import produce_avro_from_file
from .message_utils import make_rand_text, write_sample_data, compare_file_with_file, DEFAULT_SCHEMA_REGISTRY, \
    DEFAULT_BROKER


def merge_files(all_files, outfile):
    with open(f"test/testdata/{outfile}", "w") as merged_file:
        for file in all_files:
            with open(f"test/testdata/{file}") as infile:
                for row in infile:
                    merged_file.write(row)


def get_default_conf(test_name, connector_out_filename):
    return {
        "connect_name": test_name,
        "brokers": DEFAULT_BROKER,
        "topic": test_name,
        "schema_registry": DEFAULT_SCHEMA_REGISTRY,
        "filename": "test/testdata/" + connector_out_filename,
        "flush_after": 1
    }


SAMPLE_AMOUNT = 10


def test_csink_simple():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)

    wait_polls = 5

    def stop_after_some_polls(self):
        nonlocal wait_polls
        wait_polls -= 1
        if wait_polls == 0:
            return Status.STOPPED

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "on_empty_poll": stop_after_some_polls
    }

    PyConnectSinkFile(**sink_conf).run()
    compare_file_with_file(connector_out_filename, test_name)


def test_csink_stop_and_restart():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "on_message_handled": lambda s: Status.STOPPED if s.processed == 5 else None,
        "on_empty_poll": lambda x: Status.STOPPED
    }

    pc = PyConnectSinkFile(**sink_conf)
    pc.run()
    pc = PyConnectSinkFile(**sink_conf)
    pc.run()

    compare_file_with_file(connector_out_filename, test_name)


def test_csink_no_rerun():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "on_message_handled": lambda s: Status.STOPPED if s.processed == 1 else None
    }

    pc = PyConnectSinkFile(**sink_conf)
    pc.run()
    with pytest.raises(RuntimeError):
        pc.run()


def test_csink_reach_end_and_restart():
    test_name = "test_" + make_rand_text(10)
    second_test_name = "second_" + test_name
    connector_out_filename = "sink_" + test_name
    merged_filename = "merged_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "on_empty_poll": lambda x: Status.STOPPED
    }

    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once poll is empty

    write_sample_data(second_test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(second_test_name, test_name)
    pc = PyConnectSinkFile(**sink_conf)
    pc.run()

    merge_files([test_name, second_test_name], merged_filename)
    compare_file_with_file(connector_out_filename, merged_filename)


def test_csink_reach_end_recreate_and_restart():
    test_name = "test_" + make_rand_text(10)
    second_test_name = "second_" + test_name
    connector_out_filename = "sink_" + test_name
    merged_filename = "merged_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "on_empty_poll": lambda x: Status.STOPPED
    }
    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once poll is empty

    pc = PyConnectSinkFile(**sink_conf)
    write_sample_data(second_test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(second_test_name, test_name)
    pc.run()

    merge_files([test_name, second_test_name], merged_filename)
    compare_file_with_file(connector_out_filename, merged_filename)


def test_csink_fail_before_write_to_sink_then_restart():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "fail_before_counter": SAMPLE_AMOUNT / 2 + 1,
        "on_empty_poll": lambda x: Status.STOPPED
    }
    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once connector failed
    pc = PyConnectSinkFile(**sink_conf)
    pc.run()

    compare_file_with_file(test_name, connector_out_filename)


def test_csink_fail_after_write_to_sink_then_restart():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "fail_after_counter": SAMPLE_AMOUNT / 2 + 1,
        "on_empty_poll": lambda x: Status.STOPPED
    }
    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once connector failed
    pc = PyConnectSinkFile(**sink_conf)
    pc.run()

    # This should fail!
    # The sink should have once record twice since it doesn't use idempotent messages
    with pytest.raises(AssertionError):
        compare_file_with_file(test_name, connector_out_filename)


def test_flush_after_consecutive_commits_succeeds():
    test_name = "test_" + make_rand_text(10)
    second_test_name = "second_" + test_name
    merged_filename = "merged_" + test_name
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "flush_after": 1000000,
        "flush_after_consecutive_empty_polls": 3,  # This must be lower than the next line for it to be successful
        "on_empty_poll": lambda x: Status.STOPPED if x.consecutive_empty_polls > 10 else None,
    }

    pc = PyConnectSinkFile(**sink_conf)
    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once 3 consecutive empty polls have elapsed, before flushing

    pc = PyConnectSinkFile(**sink_conf)
    write_sample_data(second_test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(second_test_name, test_name)
    pc.run()

    # Since we've flushed due to flush_after_consecutive_empty_polls happening before before on_empty_poll(),
    # the processed messages should have all been committed back to kafka, so we can pick up where we left off
    merge_files([test_name, second_test_name], merged_filename)
    compare_file_with_file(merged_filename, connector_out_filename)


def test_flush_after_consecutive_commits_fails():
    test_name = "test_" + make_rand_text(10)
    second_test_name = "second_" + test_name
    merged_filename = "merged_" + test_name
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "flush_after": 1000000,
        "flush_after_consecutive_empty_polls": 10,  # This must be higher than the next line for it to fail
        "on_empty_poll": lambda x: Status.STOPPED if x.consecutive_empty_polls > 3 else None,
    }

    pc = PyConnectSinkFile(**sink_conf)
    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once 3 consecutive empty polls have elapsed, before flushing

    pc = PyConnectSinkFile(**sink_conf)
    write_sample_data(second_test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(second_test_name, test_name)
    pc.run()

    # This should fail!
    # The sink should have records twice since it doesn't use idempotent messages
    merge_files([test_name, second_test_name], merged_filename)
    with pytest.raises(AssertionError):
        compare_file_with_file(merged_filename, connector_out_filename)
