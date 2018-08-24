import os
from typing import Dict

import yaml
import pytest
import subprocess

from test.utils import rand_text


@pytest.fixture(scope='module')
def cluster_hosts():
    with open(os.path.join(TEST_DIR,
                           'testenv-docker-compose.yml'), 'r') as infile:
        yml_config = yaml.load(infile)

    hosts = {}
    for service, conf in yml_config['services'].items():
        port = conf['ports'][0].split(':')[0]
        hosts[service] = f'{service}:{port}'

    hosts['schema-registry'] = 'http://'+hosts['schema-registry']
    hosts['rest-proxy'] = 'http://'+hosts['rest-proxy']

    completed = subprocess.run(
        ['curl', '-s', hosts['rest-proxy'] + "/topics"],
        stdout=subprocess.DEVNULL)

    if completed.returncode != 0:
        pytest.fail('Kafka Cluster is not running!')

    return hosts


@pytest.fixture(
    params=[1, 2, 4],
    ids=['num_partitions=1', 'num_partitions=2', 'num_partitions=4'])
def topic(request, cluster_hosts: Dict[str, str]):
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
