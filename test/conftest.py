import itertools as it
import random
import socket
import struct
from concurrent.futures import Future
from contextlib import closing
from functools import partial
from test.utils import TestException, rand_text
from time import sleep
from typing import Any, Callable, Dict, Iterable, List, Tuple
from unittest import mock

import pytest
from confluent_kafka import DeserializingConsumer, SerializingProducer
from confluent_kafka.admin import AdminClient, ClusterMetadata, TopicMetadata
from confluent_kafka.cimpl import KafkaError, Message, NewTopic, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from loguru import logger

from pyconnect.avroparser import to_key_schema, to_value_schema
from pyconnect.config import configure_logging
from pyconnect.core import Status

configure_logging()


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: mark test to run only during end to end tests")


def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true", default=False, help="run end to end tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        # --integration given in cli: do not skip integration tests
        return
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture()
def confluent_config(cluster_config: Dict[str, str]) -> Dict[str, str]:
    return {
        "bootstrap.servers": cluster_config["broker"],
        "security.protocol": "PLAINTEXT",
        "topic.metadata.refresh.interval.ms": "250",
    }


@pytest.fixture(params=[Status.CRASHED, TestException()], ids=["Status_CRASHED", "TestException"])
def failing_callback(request):
    """
    returns a :class:`unittest.mock.Mock` object that either returns :obj:`pyconnect.core.Status.CRASHED` or raises
    :exc:`test.utils.TestException`.

    :return: Callback mock resulting in Status.CRASHED.
    """
    return mock.Mock(side_effect=it.repeat(request.param))


@pytest.fixture(scope="session")
def cluster_config() -> Dict[str, str]:
    """
    Reads the docker-compose.yml in order to determine the host names and ports of the different services necessary
    for the kafka cluster.
    :return: A map from service to url.
    """

    hosts = {"broker": "localhost:9092", "schema-registry": "http://localhost:8081", "zookeeper": "localhost:2181"}

    return hosts


CORR_ID: bytes = struct.pack(">i", 1337)
KAFKA_GENERIC_API_VERSION_REQUEST = (
    b"\x00\x00\x00\x16"  # message size
    b"\x00\x12"  # api_key 18 = api_versin request
    b"\x00\x00" + CORR_ID + b"\x00\x0c"  # api_version 0  # correlation ID  # length of client_id
    b"probe_client"  # client_id
)


@pytest.fixture(scope="session")
def assert_cluster_running(cluster_config: Dict[str, str]) -> None:
    """
    Makes sure the kafka cluster is running by checking whether we can issue an api_versions request
    """
    for _ in range(12):
        try:
            api_version_response = _send_api_version_request(cluster_config)
            assert api_version_response[:4] == CORR_ID
        except OSError as e:
            logger.info(f"Error while sending api version request: {e}")
        except AssertionError:
            logger.info(f"Broker returned wrong correlation id: {api_version_response[:4]}")
        else:
            break
        logger.info("Retrying in 10s")
        sleep(10)
    else:
        raise TimeoutError("Timed out waiting for Kafka.")
    logger.info("Kafka broker is ready.")


def _send_api_version_request(cluster_config: Dict[str, str]) -> bytes:
    with closing(socket.socket(type=socket.SOCK_STREAM)) as sock:
        addr, port = cluster_config["broker"].split(":")
        sock.settimeout(120)
        sock.connect((addr, int(port)))
        sock.sendall(KAFKA_GENERIC_API_VERSION_REQUEST)
        expected_bytes = struct.unpack(">i", sock.recv(4))[0]
        received_bytes = 0
        data = []
        while received_bytes < expected_bytes:
            data.append(sock.recv(1024))
            received_bytes += len(data[-1])
        sock.shutdown(socket.SHUT_RDWR)

    return b"".join(data)


@pytest.fixture(scope="session")
def running_cluster_config(cluster_config: Dict[str, str], assert_cluster_running) -> Dict[str, str]:
    """
    Reads the docker-compose.yml in order to determine the host names and ports of the different services necessary
    for the kafka cluster.
    Also makes sure that the cluster is running.
    :return: A map from service to url.
    """
    return cluster_config


@pytest.fixture()
def confluent_admin_client(confluent_config: Dict[str, str]) -> AdminClient:
    return AdminClient(confluent_config)


@pytest.fixture(params=[1, 2, 4], ids=["num_partitions=1", "num_partitions=2", "num_partitions=4"])
def topic_and_partitions(
    request, confluent_admin_client: AdminClient, running_cluster_config: Dict[str, str]
) -> Iterable[Tuple[str, int]]:
    """
    Creates a kafka topic consisting of a random 5 character string and being partition into 1, 2 or 4 partitions.
    Then it yields the tuple (topic, n_partitions).

    Prints topic information before and after topic was used by a test.
    :return: Topic and number of partitions within it.
    """
    topic_id = rand_text(5)

    partitions = request.param

    results: Dict[str, Future] = confluent_admin_client.create_topics(
        [NewTopic(topic_id, num_partitions=partitions, replication_factor=1)], operation_timeout=45
    )
    for future in results.values():
        future.result(timeout=60)

    yield topic_id, partitions

    confluent_admin_client.delete_topics([NewTopic(topic_id, num_partitions=partitions, replication_factor=1)])


@pytest.fixture
def plain_avro_producer(
    running_cluster_config: Dict[str, str], topic_and_partitions: Tuple[str, int], records
) -> SerializingProducer:
    """
    Creates a plain `confluent_kafka.avro.AvroProducer` that can be used to publish messages.
    """
    topic_id, _ = topic_and_partitions

    key, value = records[0]

    schema_registry_client = SchemaRegistryClient({"url": running_cluster_config["schema-registry"]})
    key_schema = to_key_schema(key)
    avro_key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str=key_schema)
    value_schema = to_value_schema(value)
    avro_value_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str=value_schema)

    producer_config = {
        "bootstrap.servers": running_cluster_config["broker"],
        "key.serializer": avro_key_serializer,
        "value.serializer": avro_value_serializer,
    }

    producer = SerializingProducer(producer_config)

    producer.produce = partial(producer.produce, topic=topic_id)

    return producer


@pytest.fixture
def plain_avro_consumer(running_cluster_config: Dict[str, str], topic_and_partitions: Tuple[str, int]):
    topic_id, _ = topic_and_partitions
    schema_registry_client = SchemaRegistryClient({"url": running_cluster_config["schema-registry"]})
    key_deserializer = AvroDeserializer(schema_registry_client)
    value_deserializer = AvroDeserializer(schema_registry_client)
    config = {
        "bootstrap.servers": running_cluster_config["broker"],
        "group.id": f"{topic_id}_consumer",
        "key.deserializer": key_deserializer,
        "value.deserializer": value_deserializer,
        "enable.partition.eof": False,
        "default.topic.config": {"auto.offset.reset": "earliest"},
        "allow.auto.create.topics": True,
    }
    consumer = DeserializingConsumer(config)
    consumer.subscribe([topic_id])
    consumer.list_topics()
    return consumer


Record = Tuple[Any, Any]
RecordList = List[Record]


@pytest.fixture
def produced_messages(
    records: RecordList,
    plain_avro_producer,
    plain_avro_consumer,
    topic_and_partitions: Tuple[str, int],
    running_cluster_config: Dict[str, str],
    consume_all,
) -> Iterable[List[Tuple[str, dict]]]:
    """
    Creates 15 random messages, produces them to the currently active topic and then yields them for the test.
    """
    topic_id, partitions = topic_and_partitions

    for key, value in records:
        plain_avro_producer.produce(key=key, value=value)

    plain_avro_producer.flush()

    cluster_metadata: ClusterMetadata = plain_avro_consumer.list_topics(topic=topic_id)
    topic_metadata: TopicMetadata = cluster_metadata.topics[topic_id]
    logger.info(f"Topic partitions: {topic_metadata.partitions.keys()}")
    assert partitions == len(topic_metadata.partitions.keys()), "Not all partitions present"
    offsets = 0
    for partition in topic_metadata.partitions.keys():
        _, ho = plain_avro_consumer.get_watermark_offsets(TopicPartition(topic_id, partition))
        offsets += ho

    assert len(records) == offsets, ""
    yield records


@pytest.fixture
def message_factory() -> Iterable[Callable[..., Message]]:
    """
    Creates a factory for mocked :class:`confluent_kafka.Message` object.
    """
    with mock.patch("test.conftest.Message", autospec=True):

        def message_factory_(key="key", value="value", topic="topic", offset=0, partition=0, error=None):
            msg = Message()
            msg.error.return_value = error
            msg.topic.return_value = topic
            msg.partition.return_value = partition
            msg.offset.return_value = offset
            msg.key.return_value = key
            msg.value.return_value = value
            return msg

        yield message_factory_


@pytest.fixture
def error_message_factory(message_factory) -> Callable[..., Message]:
    """
    Creates a factory for mockec :class:`confluent_kafka.Message` that return a :class:`confluent_kafka.KafkaError`
    when :meth:`confluent_kafka.Message.error` is called on them.
    """
    with mock.patch("test.conftest.KafkaError", autospec=True):

        def error_message_factory_(error_code=None):
            error = KafkaError()
            error.code.return_value = error_code
            return message_factory(error=error)

        yield error_message_factory_


@pytest.fixture
def eof_message(error_message_factory) -> Message:
    """
    Returns an EOF message.
    I.e. a :class:`confluent_kafka.Message` that holds a :class:`confluent_kafka.KafkaError` with error code
    :const:`confluent_kafka.KafkaError._PARTITION_EOF`.
    """
    return error_message_factory(error_code=KafkaError._PARTITION_EOF)


ConsumeAll = Callable[..., RecordList]


@pytest.fixture
def records() -> RecordList:
    """
    Just a list of simple records, ready to be used as messages.
    """
    return [(rand_text(8), {"a": rand_text(64), "b": random.randint(0, 1000)}) for _ in range(15)]


@pytest.fixture
def consume_all(plain_avro_consumer) -> Iterable[ConsumeAll]:
    """
    Creates a function that consumes and returns all messages for the current test's topic.
    """

    def consume_all_() -> RecordList:
        records = []
        while True:
            msg = plain_avro_consumer.poll(timeout=100)
            if msg is None:
                break
            records.append((msg.key(), msg.value()))

        return records

    yield consume_all_
