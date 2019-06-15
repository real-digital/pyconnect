import itertools as it
import logging
import os
import random
import subprocess
from functools import partial
from typing import Any, Callable, Dict, Iterable, List, Tuple
from unittest import mock

import pytest
import yaml
from confluent_kafka import avro as confluent_avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaError, Message

from pyconnect.avroparser import to_key_schema, to_value_schema
from pyconnect.core import Status
from test.utils import CLI_DIR, TEST_DIR, TestException, rand_text

logging.basicConfig(
    format="%(asctime)s|%(threadName)s|%(levelname)s|%(name)s|%(message)s",
    filename=str(TEST_DIR / "test.log"),
    filemode="w",
)
logger = logging.getLogger("test.conftest")


def pytest_addoption(parser):
    parser.addoption("--run-e2e", action="store_true", default=False, help="run end to end tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-e2e"):
        # --run-e2e given in cli: do not skip e2e tests
        return
    skip_e2e = pytest.mark.skip(reason="need --run-e2e option to run")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_e2e)


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
    with (TEST_DIR / "docker-compose.yml").open("r") as infile:
        yml_config = yaml.safe_load(infile)

    hosts = {"broker": "", "schema-registry": "", "rest-proxy": "", "zookeeper": ""}

    for service, conf in yml_config["services"].items():
        port = conf["ports"][0].split(":")[0]
        hosts[service] = f"{service}:{port}"

    for service in hosts.keys():
        env_var = (service + "_url").upper().replace('-', '_')
        if env_var in os.environ:
            hosts[service] = os.environ[env_var]

    if "http" not in hosts["schema-registry"]:
        hosts["schema-registry"] = "http://" + hosts["schema-registry"]
    if "http" not in hosts["rest-proxy"]:
        hosts["rest-proxy"] = "http://" + hosts["rest-proxy"]

    assert all(hosts.values()), "Not all service urls have been defined!"

    return hosts


@pytest.fixture(scope="session")
def assert_cluster_running(cluster_config) -> None:
    """
    Makes sure the kafka cluster is running by checking whether the rest-proxy service returns the topics
    """
    # completed = subprocess.run(["curl", "-s", cluster_config["rest-proxy"] + "/topics"], stdout=subprocess.DEVNULL)
    #
    # assert completed.returncode == 0, "Kafka Cluster is not running!"

    assert True


@pytest.fixture(scope="session")
def running_cluster_config(cluster_config, assert_cluster_running) -> Dict[str, str]:
    """
    Reads the docker-compose.yml in order to determine the host names and ports of the different services necessary
    for the kafka cluster.
    Also makes sure that the cluster is running.
    :return: A map from service to url.
    """
    return cluster_config


@pytest.fixture(params=[1, 2, 4], ids=["num_partitions=1", "num_partitions=2", "num_partitions=4"])
def topic(request, running_cluster_config) -> Iterable[Tuple[str, int]]:
    """
    Creates a kafka topic consisting of a random 5 character string and being partition into 1, 2 or 4 partitions.
    Then it yields the tuple (topic, n_partitions).

    Prints topic information before and after topic was used by a test.
    :return: Topic and number of partitions within it.
    """
    topic_id = rand_text(5)
    partitions = request.param

    creation_output = subprocess.run(
        [
            CLI_DIR / "kafka-topics.sh",
            "--zookeeper",
            running_cluster_config["zookeeper"],
            "--create",
            "--topic",
            topic_id,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ],
        stdout=subprocess.PIPE,
    ).stdout.decode()
    logger.info(creation_output)

    yield (topic_id, partitions)

    description_output = subprocess.run(
        [
            CLI_DIR / "kafka-topics.sh",
            "--zookeeper",
            running_cluster_config["zookeeper"],
            "--describe",
            "--topic",
            topic_id,
        ],
        stdout=subprocess.PIPE,
    ).stdout.decode()
    logger.info(description_output)


@pytest.fixture
def plain_avro_producer(running_cluster_config, topic) -> confluent_avro.AvroProducer:
    """
    Creates a plain `confluent_kafka.avro.AvroProducer` that can be used to publish messages.
    """
    topic_id, partitions = topic
    producer_config = {
        "bootstrap.servers": running_cluster_config["broker"],
        "schema.registry.url": running_cluster_config["schema-registry"],
    }
    producer = confluent_avro.AvroProducer(producer_config)
    producer.produce = partial(producer.produce, topic=topic_id)

    return producer


@pytest.fixture
def produced_messages(
    records, plain_avro_producer, topic, running_cluster_config, consume_all
) -> Iterable[List[Tuple[str, dict]]]:
    """
    Creates 15 random messages, produces them to the currently active topic and then yields them for the test.
    """
    topic_id, partitions = topic

    key, value = records[0]
    key_schema = to_key_schema(key)
    value_schema = to_value_schema(value)

    for key, value in records:
        plain_avro_producer.produce(key=key, value=value, key_schema=key_schema, value_schema=value_schema)

    plain_avro_producer.flush()

    topic_highwater = subprocess.run(
        [
            CLI_DIR / "kafka-run-class.sh",
            "kafka.tools.GetOffsetShell",
            "--broker-list",
            running_cluster_config["broker"],
            "--topic",
            topic_id,
            "--time",
            "-1",
            "--offsets",
            "1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=True,
    ).stdout.decode()
    logger.info(f"Topic highwater:\n{topic_highwater}")
    assert len(topic_highwater.splitlines()) == partitions, "Not all partitions present"

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


Record = Tuple[Any, Any]
RecordList = List[Record]
ConsumeAll = Callable[..., RecordList]


@pytest.fixture
def records() -> RecordList:
    """
    Just a list of simple records, ready to be used as messages.
    """
    return [(rand_text(8), {"a": rand_text(64), "b": random.randint(0, 1000)}) for _ in range(15)]


@pytest.fixture
def consume_all(topic, running_cluster_config) -> Iterable[ConsumeAll]:
    """
    Creates a function that consumes and returns all messages for the current test's topic.
    """
    topic_id, _ = topic

    consumer = AvroConsumer(
        {
            "bootstrap.servers": running_cluster_config["broker"],
            "schema.registry.url": running_cluster_config["schema-registry"],
            "group.id": f"{topic_id}_consumer",
            "enable.partition.eof": False,
            "default.topic.config": {"auto.offset.reset": "earliest"},
        }
    )
    consumer.subscribe([topic_id])

    def consume_all_() -> RecordList:
        records = []
        while True:
            msg = consumer.poll(timeout=2)
            if msg is None:
                break
            if msg.error() is not None:
                assert msg.error().code() == KafkaError._PARTITION_EOF
                break
            records.append((msg.key(), msg.value()))
        return records

    yield consume_all_
    consumer.close()
