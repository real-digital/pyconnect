import json
import pathlib
import shutil
import subprocess
from typing import Dict, List, Tuple

import pytest

from .conftest import ConsumeAll, RecordList
from .utils import ROOT_DIR, compare_lists_unordered

EXAMPLES_DIR = ROOT_DIR / "examples"


@pytest.fixture
def tmp_with_pyconnect(tmpdir: pathlib.Path) -> Tuple[pathlib.Path, pathlib.Path]:
    tmpdir = pathlib.Path(tmpdir).absolute()
    venv_name = ".test_venv"
    venv_bin = tmpdir / venv_name / "bin"

    subprocess.run(["virtualenv", f"--python=python3", venv_name], cwd=tmpdir, check=True)
    subprocess.run([venv_bin / "pip", "install", "-U", "pip"], cwd=tmpdir, check=True)
    subprocess.run([venv_bin / "pip", "install", ROOT_DIR], cwd=tmpdir, check=True)
    return tmpdir, venv_bin


@pytest.mark.integration
def test_file_sink_example(
    running_cluster_config: Dict[str, str],
    topic_and_partitions: Tuple[str, int],
    produced_messages: List[Tuple[str, dict]],
    tmp_with_pyconnect: Tuple[pathlib.Path, pathlib.Path],
):
    tmpdir, venv_bin = tmp_with_pyconnect
    sinkfile = tmpdir / "sink_dir" / "sinkfile"

    env_vars = {
        "PYCONNECT_BOOTSTRAP_SERVERS": running_cluster_config["broker"],
        "PYCONNECT_SCHEMA_REGISTRY": running_cluster_config["schema-registry"],
        "PYCONNECT_TOPICS": topic_and_partitions[0],
        "PYCONNECT_GROUP_ID": "testgroup",
        "PYCONNECT_SINK_DIRECTORY": sinkfile.parent,
        "PYCONNECT_SINK_FILENAME": sinkfile.name,
    }

    shutil.copy(EXAMPLES_DIR / "file_sink" / "file_sink.py", tmpdir)

    subprocess.run(
        [venv_bin / "python", "file_sink.py", "--config", "env", "--loglevel", "DEBUG"],
        env=env_vars,
        cwd=tmpdir,
        check=True,
        timeout=300,
    )

    filedata = sinkfile.read_text()

    saved_messages = [(record["key"], record["value"]) for record in map(json.loads, filedata.splitlines())]

    compare_lists_unordered(produced_messages, saved_messages)


@pytest.mark.integration
def test_file_source_example(
    records: RecordList,
    running_cluster_config: Dict[str, str],
    topic_and_partitions: Tuple[str, int],
    consume_all: ConsumeAll,
    tmp_with_pyconnect: Tuple[pathlib.Path, pathlib.Path],
):
    tmpdir, venv_bin = tmp_with_pyconnect
    source_file = tmpdir / "source_dir" / "sourcefile"

    topic_id, _ = topic_and_partitions

    env_vars = {
        "PYCONNECT_BOOTSTRAP_SERVERS": running_cluster_config["broker"],
        "PYCONNECT_SCHEMA_REGISTRY": running_cluster_config["schema-registry"],
        "PYCONNECT_TOPIC": topic_id,
        "PYCONNECT_OFFSET_TOPIC": f"{topic_id}_offset_topic",
        "PYCONNECT_SOURCE_DIRECTORY": source_file.parent,
        "PYCONNECT_SOURCE_FILENAME": source_file.name,
    }

    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_data = "\n".join(json.dumps({"key": key, "value": value}) for key, value in records)
    source_file.write_text(source_data)

    shutil.copy(EXAMPLES_DIR / "file_source" / "file_source.py", tmpdir)
    subprocess.run(
        [venv_bin / "python", "file_source.py", "--config", "env", "--loglevel", "DEBUG"],
        env=env_vars,
        cwd=tmpdir,
        check=True,
        timeout=300,
    )

    published_records = consume_all()

    compare_lists_unordered(records, published_records)
