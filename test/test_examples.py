import json
import pathlib
import shutil
import subprocess
import sys

import pytest

from .utils import ROOT_DIR

EXAMPLES_DIR = ROOT_DIR / 'examples'


@pytest.fixture
def tmp_with_pyconnect(tmpdir):
    python = f'python{sys.version_info.major}.{sys.version_info.minor}'
    subprocess.run(['virtualenv', f'--python={python}', '.test_venv'], cwd=tmpdir, check=True)
    subprocess.run(['.test_venv/bin/pip', 'install', ROOT_DIR], cwd=tmpdir, check=True)
    return pathlib.Path(tmpdir)


@pytest.mark.e2e
def test_file_sink_example(cluster_hosts, topic, produced_messages, tmp_with_pyconnect):
    env_vars = {
        'PYCONNECT_BOOTSTRAP_SERVERS': cluster_hosts['broker'],
        'PYCONNECT_SCHEMA_REGISTRY': cluster_hosts['schema-registry'],
        'PYCONNECT_TOPICS': topic[0],
        'PYCONNECT_GROUP_ID': 'testgroup',
        'PYCONNECT_SINK_DIRECTORY': tmp_with_pyconnect / 'sink_dir',
        'PYCONNECT_SINK_FILENAME': 'sinkfile'
    }

    shutil.copy(EXAMPLES_DIR / 'file_sink' / 'file_sink.py', tmp_with_pyconnect)
    subprocess.run(['.test_venv/bin/python', 'file_sink.py', '--config', 'env'], env=env_vars,
                   cwd=tmp_with_pyconnect, check=True)

    with open(tmp_with_pyconnect / 'sink_dir' / 'sinkfile', 'r') as infile:
        lines = infile.readlines()

    saved_records = [json.loads(line) for line in lines]

    for key, value in produced_messages:
        assert {'key': key, 'value': value} in saved_records


@pytest.mark.e2e
def test_file_source_example(records, cluster_hosts, topic, consume_all, tmp_with_pyconnect):
    source_dir = tmp_with_pyconnect / 'source_dir'
    env_vars = {
        'PYCONNECT_BOOTSTRAP_SERVERS': cluster_hosts['broker'],
        'PYCONNECT_SCHEMA_REGISTRY': cluster_hosts['schema-registry'],
        'PYCONNECT_TOPIC': topic[0],
        'PYCONNECT_OFFSET_TOPIC': topic[0] + '_offset_topic',
        'PYCONNECT_SOURCE_DIRECTORY': source_dir,
        'PYCONNECT_SOURCE_FILENAME': 'sourcefile'
    }

    source_dir.mkdir(parents=True, exist_ok=True)
    with open(source_dir / 'sourcefile', 'w') as outfile:
        for key, value in records:
            outfile.write(json.dumps({'key': key, 'value': value}) + '\n')

    shutil.copy(EXAMPLES_DIR / 'file_source' / 'file_source.py', tmp_with_pyconnect)
    subprocess.run(['.test_venv/bin/python', 'file_source.py', '--config', 'env'], env=env_vars,
                   cwd=tmp_with_pyconnect, check=True)

    published_records = consume_all()

    for record in records:
        assert record in published_records
