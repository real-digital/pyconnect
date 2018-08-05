from random import choice
from string import ascii_letters
import json

TIMEOUT = 1.0
make_rand_text = lambda x: "".join(choice(ascii_letters) for _ in range(x))
_BASE_CONF = {"bootstrap.servers": "localhost"}


def get_producer_conf():
    return _BASE_CONF


def get_avro_producer_conf():
    return {
        'schema.registry.url': 'http://127.0.0.1:8081',
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


def create_sample_data(filename, sample_size=1000):
    data = ""
    for i in range(sample_size):
        sample = {"nr": i, "text": make_rand_text(5), "nested": {"text": make_rand_text(7)}}
        data += json.dumps(sample) + "\n"
    with open(f"test/testdata/{filename}.sample", "w") as outfile:
        outfile.write(data)


def read_sample_data(filename):
    with open(f"test/testdata/{filename}.sample") as infile:
        return infile.readlines()


def producer_callback(err, _msg):
    if err is not None:
        print('Failed to produce: {}'.format(err))


def compare_data_with_file(data, filename):
    orig_data = read_sample_data(filename)
    for d, o in zip(data, orig_data):
        assert d == o, f"Consumed data [{d}] is not the same as original sample data [{o}]!"
