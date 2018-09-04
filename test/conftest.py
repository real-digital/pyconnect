import itertools as it
import os
import subprocess
from typing import Callable, Dict, Iterable, Tuple
from unittest import mock

import pytest
import yaml
from confluent_kafka.cimpl import KafkaError, Message

from pyconnect.core import Status
from test.utils import CLI_DIR, TEST_DIR, TestException, rand_text


def pytest_addoption(parser):
    parser.addoption(
        "--run-e2e", action="store_true", default=False,
        help="run end to end tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-e2e"):
        # --run-e2e given in cli: do not skip e2e tests
        return
    skip_e2e = pytest.mark.skip(reason="need --run-e2e option to run")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_e2e)


@pytest.fixture(params=[Status.CRASHED, TestException()],
                ids=['Status_CRASHED', 'TestException'])
def failing_callback(request):
    """
    returns a :class:`unittest.mock.Mock` object that either returns :obj:`pyconnect.core.Status.CRASHED` or raises
    :exc:`test.utils.TestException`.

    :return: Callback mock resulting in Status.CRASHED.
    """
    return mock.Mock(side_effect=it.repeat(request.param))


@pytest.fixture(scope='module')
def cluster_hosts() -> Dict[str, str]:
    """
    Reads the docker-compose.yml in order to determine the host names and ports of the different services necessary
    for the kafka cluster.
    :return: A map from service to url.
    """
    with open(os.path.join(TEST_DIR,
                           'testenv-docker-compose.yml'), 'r') as infile:
        yml_config = yaml.load(infile)

    hosts = {
        'broker': '',
        'schema-registry': '',
        'rest-proxy': '',
        'zookeeper': ''
    }

    for service, conf in yml_config['services'].items():
        port = conf['ports'][0].split(':')[0]
        hosts[service] = f'{service}:{port}'

    for service in hosts.keys():
        env_var = service + '_url'
        if env_var in os.environ:
            hosts[service] = os.environ[env_var]

    if 'http' not in hosts['schema-registry']:
        hosts['schema-registry'] = 'http://' + hosts['schema-registry']
    if 'http' not in hosts['rest-proxy']:
        hosts['rest-proxy'] = 'http://' + hosts['rest-proxy']

    assert all(hosts.values()), 'Not all service urls have been defined!'

    completed = subprocess.run(['curl', '-s', hosts['rest-proxy'] + "/topics"], stdout=subprocess.DEVNULL)

    if completed.returncode != 0:
        pytest.fail('Kafka Cluster is not running!')

    return hosts


@pytest.fixture(
    params=[1, 2, 4],
    ids=['num_partitions=1', 'num_partitions=2', 'num_partitions=4'])
def topic(request, cluster_hosts) -> Iterable[Tuple[str, int]]:
    """
    Creates a kafka topic consisting of a random 5 character string and being partition into 1, 2 or 4 partitions.
    Then it yields the tuple (topic, n_partitions).

    Prints topic information before and after topic was used by a test.
    :return: Topic and number of partitions within it.
    """
    topic_id = rand_text(5)
    partitions = request.param

    subprocess.call([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--create', '--topic', topic_id,
        '--partitions', str(partitions),
        '--replication-factor', '1'
    ])

    yield (topic_id, partitions)

    subprocess.call([
        os.path.join(CLI_DIR, 'kafka-topics.sh'),
        '--zookeeper', cluster_hosts['zookeeper'],
        '--describe', '--topic', topic_id
    ])


@pytest.fixture
def message_factory() -> Iterable[Callable[..., Message]]:
    """
    Creates a factory for mocked :class:`confluent_kafka.Message` object.
    """
    with mock.patch('test.conftest.Message', autospec=True):
        def message_factory_(key='key', value='value', topic='topic', offset=0, partition=0, error=None):
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
    with mock.patch('test.conftest.KafkaError', autospec=True):
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
