import json
import pathlib
import shutil
import subprocess

import pytest

from .utils import ROOT_DIR

EXAMPLES_DIR = ROOT_DIR / 'examples'


@pytest.fixture
def tmp_with_pyconnect(tmpdir):
    tmpdir = pathlib.Path(tmpdir).absolute()
    venv_name = '.test_venv'
    venv_bin = tmpdir / venv_name / 'bin'

    subprocess.run(['python', '-m', 'venv', venv_name], cwd=tmpdir, check=True)
    subprocess.run([venv_bin / 'pip', 'install', ROOT_DIR], cwd=tmpdir, check=True)
    return tmpdir, venv_bin


@pytest.mark.e2e
def test_file_sink_example(cluster_hosts, topic, produced_messages, tmp_with_pyconnect):
    tmpdir, venv_bin = tmp_with_pyconnect
    sinkfile = tmpdir / 'sink_dir' / 'sinkfile'

    env_vars = {
        'PYCONNECT_BOOTSTRAP_SERVERS': cluster_hosts['broker'],
        'PYCONNECT_SCHEMA_REGISTRY': cluster_hosts['schema-registry'],
        'PYCONNECT_TOPICS': topic[0],
        'PYCONNECT_GROUP_ID': 'testgroup',
        'PYCONNECT_SINK_DIRECTORY': sinkfile.parent,
        'PYCONNECT_SINK_FILENAME': sinkfile.name
    }

    shutil.copy(EXAMPLES_DIR / 'file_sink' / 'file_sink.py', tmpdir)

    subprocess.run([venv_bin / 'python', 'file_sink.py', '--config', 'env'], env=env_vars,
                   cwd=tmpdir, check=True)

    filedata = sinkfile.read_text()

    saved_records = [json.loads(line) for line in filedata.splitlines()]

    for key, value in produced_messages:
        assert {'key': key, 'value': value} in saved_records


@pytest.mark.e2e
def test_file_source_example(records, cluster_hosts, topic, consume_all, tmp_with_pyconnect):
    tmpdir, venv_bin = tmp_with_pyconnect
    source_file = tmpdir / 'source_dir' / 'sourcefile'

    env_vars = {
        'PYCONNECT_BOOTSTRAP_SERVERS': cluster_hosts['broker'],
        'PYCONNECT_SCHEMA_REGISTRY': cluster_hosts['schema-registry'],
        'PYCONNECT_TOPIC': topic[0],
        'PYCONNECT_OFFSET_TOPIC': topic[0] + '_offset_topic',
        'PYCONNECT_SOURCE_DIRECTORY': source_file.parent,
        'PYCONNECT_SOURCE_FILENAME': source_file.name
    }

    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_data = '\n'.join(
        json.dumps({'key': key, 'value': value})
        for key, value
        in records
    )
    source_file.write_text(source_data)

    shutil.copy(EXAMPLES_DIR / 'file_source' / 'file_source.py', tmpdir)
    subprocess.run([venv_bin / 'python', 'file_source.py', '--config', 'env'], env=env_vars,
                   cwd=tmpdir, check=True)

    published_records = consume_all()

    for record in records:
        assert record in published_records
