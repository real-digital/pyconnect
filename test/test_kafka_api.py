from string import ascii_letters
from random import choice
import json
# noinspection PyProtectedMember
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.avro import AvroProducer, AvroConsumer, loads

from pyconnect.avroparser import create_schema_from_record

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


def produce_json(filename, topic):
    producer = Producer(get_producer_conf())
    sample_rows = read_sample_data(filename)
    for row in sample_rows:
        producer.produce(topic, row.encode("utf-8"), callback=producer_callback)
    producer.flush()


def produce_avro(filename, topic):
    sample_rows = read_sample_data(filename)

    single_sample = json.loads(sample_rows[0])
    schema = loads(json.dumps(create_schema_from_record(f"namespace_{topic}", single_sample)))

    producer = AvroProducer(
        get_avro_producer_conf(),
        default_value_schema=schema
    )

    for row in sample_rows:
        producer.produce(topic=topic, value=row.encode("utf-8"), callback=producer_callback)
    producer.flush()


def consume_json(topic, consumer):
    consumer.subscribe([topic])
    data = []
    while True:
        msg = consumer.poll(TIMEOUT)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            else:
                print("FATAL::", msg.error())
                break
        data.append(msg.value().decode('utf-8'))
    consumer.close()
    return data


def compare_data_with_file(data, filename):
    orig_data = read_sample_data(filename)
    for d, o in zip(data, orig_data):
        assert d == o, f"Consumed data [{d}] is not the same as original sample data [{o}]!"


def test_cluster_json():
    test_name = "test_" + make_rand_text(10)
    consumer = Consumer(get_consumer_conf())
    create_sample_data(test_name, sample_size=50)
    produce_json(test_name, test_name)
    sample_data = consume_json(test_name, consumer)
    compare_data_with_file(sample_data, test_name)


def test_cluster_avro():
    test_name = "test_" + make_rand_text(10)
    consumer = AvroConsumer(get_avro_consumer_conf())
    create_sample_data(test_name, sample_size=50)
    produce_avro(test_name, test_name)
    sample_data = consume_json(test_name, consumer)
    compare_data_with_file(sample_data, test_name)
