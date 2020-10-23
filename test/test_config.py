from datetime import timedelta
from unittest import mock

from pyconnect.config import SinkConfig


# TODO add tests for the other config loaders
def test_env_loader():
    env_vars = {
        "PYCONNECT_BOOTSTRAP_SERVERS": '["broker:9092"]',
        "PYCONNECT_SCHEMA_REGISTRY": "http://schema-registry:8082",
        "PYCONNECT_TOPICS": '["testtopic"]',
        "PYCONNECT_GROUP_ID": "testgroup",
        "PYCONNECT_OFFSET_COMMIT_INTERVAL": "720",
    }
    with mock.patch.dict("pyconnect.config.os.environ", env_vars):
        config: SinkConfig = SinkConfig()

        assert config.bootstrap_servers == ["broker:9092"]
        assert config.schema_registry == "http://schema-registry:8082"
        assert config.topics == ["testtopic"]
        assert config.group_id == "testgroup"
        assert config.offset_commit_interval == timedelta(minutes=12)
