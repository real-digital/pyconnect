import json

from confluent_kafka.avro import AvroProducer, AvroConsumer, loads
from confluent_kafka.cimpl import Producer, Consumer, KafkaError

from pyconnect.avroparser import create_schema_from_record
from .message_utils import TIMEOUT, make_rand_text, get_producer_conf, get_avro_producer_conf, get_consumer_conf, \
    get_avro_consumer_conf, write_sample_data, read_sample_data, producer_callback, compare_data_with_file


# Producers and consumers

def produce_json(filename, topic):
    producer = Producer(get_producer_conf())
    sample_rows = read_sample_data(filename)
    for row in sample_rows:
        producer.produce(topic, row.encode("utf-8"), callback=producer_callback)
    producer.flush()


def produce_avro_from_file(filename, topic):
    sample_rows = read_sample_data(filename)

    single_sample = json.loads(sample_rows[0])
    schema = loads(json.dumps(create_schema_from_record(f"namespace_{topic}", single_sample)))

    producer = AvroProducer(
        get_avro_producer_conf(),
        default_value_schema=schema
    )

    for row in sample_rows:
        producer.produce(topic=topic, value=json.loads(row), callback=producer_callback)
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


def consume_avro(topic, consumer):
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
        data.append(json.dumps(msg.value()) + "\n")  # newline so the comparison with the string is correct
        # TODO FIXME have to remove the newlines, also from the normal producer
    consumer.close()
    return data


# Tests


def test_cluster_json():
    test_name = "test_" + make_rand_text(10)
    consumer = Consumer(get_consumer_conf())
    write_sample_data(test_name, sample_size=50)
    produce_json(test_name, test_name)
    sample_data = consume_json(test_name, consumer)
    compare_data_with_file(sample_data, test_name)


def test_cluster_avro():
    test_name = "test_" + make_rand_text(10)
    consumer = AvroConsumer(get_avro_consumer_conf())
    write_sample_data(test_name, sample_size=50)
    produce_avro_from_file(test_name, test_name)
    sample_data = consume_avro(test_name, consumer)
    compare_data_with_file(sample_data, test_name)
