from pyconnect.pyconnectsink import PyConnectSinkFile, Status
from test.test_kafka_api import produce_avro
from .message_utils import make_rand_text, write_sample_data, compare_file_with_file, DEFAULT_SCHEMA_REGISTRY, DEFAULT_BROKER


def test_csink_simple():
    test_name = "test_" + make_rand_text(10)
    file_name = "sink_" + test_name
    samples = 10

    write_sample_data(test_name, sample_size=samples)
    produce_avro(test_name, test_name)

    wait_polls = 5
    def stop_after_some_polls(self):
        nonlocal wait_polls
        wait_polls -= 1
        if wait_polls == 0:
            return Status.STOPPED

    sink_conf = {
        "connect_name": test_name,
        "brokers": DEFAULT_BROKER,
        "topic": test_name,
        "schema_registry": DEFAULT_SCHEMA_REGISTRY,
        "filename": "test/testdata/" + file_name,
        "flush_after": samples,
        "on_empty_poll": stop_after_some_polls
    }

    PyConnectSinkFile(**sink_conf).run()
    compare_file_with_file(file_name, test_name)
