from pyconnect.pyconnectsinkfile import PyConnectSinkFile
from pyconnect.pyconnectsink import Status
from test.test_kafka_api import produce_avro_from_file
from .message_utils import make_rand_text, write_sample_data, compare_file_with_file, DEFAULT_SCHEMA_REGISTRY, \
    DEFAULT_BROKER
import pytest


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


@pytest.mark.skip()
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


@pytest.mark.skip()
def test_csink_stop_and_resume():
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
    pc.run()

    compare_file_with_file(connector_out_filename, test_name)


@pytest.mark.skip()
def test_csink_reach_end_and_resume():
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
    pc.run()

    merge_files([test_name, second_test_name], merged_filename)
    compare_file_with_file(connector_out_filename, merged_filename)


@pytest.mark.skip()
def test_csink_reach_end_recreate_and_resume():
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


def test_csink_fail_before_write_to_sink_then_resume():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "fail_before_counter": 3,
        "on_empty_poll": lambda x: Status.STOPPED
    }
    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once connector failed
    pc.run()

    compare_file_with_file(test_name, connector_out_filename)

@pytest.mark.skip()
def test_csink_fail_after_write_to_sink_then_resume():
    test_name = "test_" + make_rand_text(10)
    connector_out_filename = "sink_" + test_name

    sink_conf = {
        **get_default_conf(test_name, connector_out_filename),
        "fail_after_counter": 3,
        "on_empty_poll": lambda x: Status.STOPPED
    }
    pc = PyConnectSinkFile(**sink_conf)

    write_sample_data(test_name, sample_size=SAMPLE_AMOUNT)
    produce_avro_from_file(test_name, test_name)
    pc.run()
    # will return once connector failed
    pc.run()

    compare_file_with_file(test_name, connector_out_filename)
