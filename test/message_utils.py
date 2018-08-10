from random import choice
from string import ascii_letters
import json
from itertools import zip_longest

TIMEOUT = 1.0
DEFAULT_BROKER = "localhost"
DEFAULT_SCHEMA_REGISTRY = 'http://127.0.0.1:8081'
_BASE_CONF = {"bootstrap.servers": DEFAULT_BROKER}


def make_rand_text(length):
    return "".join(choice(ascii_letters) for _ in range(length))


def get_producer_conf():
    return _BASE_CONF


def get_avro_producer_conf():
    return {
        'schema.registry.url': DEFAULT_SCHEMA_REGISTRY,
        **get_producer_conf()
    }


def get_consumer_conf():
    return {
        "group.id": make_rand_text(10),
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        **_BASE_CONF
    }


def get_avro_consumer_conf():
    return {'schema.registry.url': 'http://127.0.0.1:8081', **get_consumer_conf()}


def create_sample_data(sample_size=1000):
    data = []
    for i in range(sample_size):
        sample = {"nr": i, "text": make_rand_text(5), "nested": {"text": make_rand_text(7)}}
        data.append(sample)
    return data


def write_sample_data(filename, sample_size=1000):
    data = create_sample_data(sample_size)
    data_str = "\n".join(json.dumps(d) for d in data) + "\n"
    with open(f"test/testdata/{filename}", "w") as outfile:
        outfile.write(data_str)


def read_sample_data(filename):
    with open(f"test/testdata/{filename}") as infile:
        return infile.readlines()


def producer_callback(err, _msg):
    if err is not None:
        print('Failed to produce: {}'.format(err))


def compare_data_with_file(data, filename):
    orig_data = read_sample_data(filename)
    for d, o in zip_longest(data, orig_data):
        assert d == o, f"Consumed data [{d}] is not the same as original sample data [{o}]!"


def compare_file_with_file(file_1, file_2):
    for d, o in zip_longest(read_sample_data(file_1), read_sample_data(file_2)):
        assert d == o, f"Consumed data [{d}] is not the same as original sample data [{o}]!"
