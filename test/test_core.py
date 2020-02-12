from pyconnect.core import hash_sensitive_values


def test_hash_sensitive_values_hashes():
    config = {"sasl.password": "unhashed password", "regular_key": "regular value"}
    config_copy = config.copy()
    hashed_config = hash_sensitive_values(config_copy)
    assert config == config_copy
    assert hashed_config["sasl.password"] != config_copy["sasl.password"]
    assert hashed_config["regular_key"] == config_copy["regular_key"]


def test_hash_sensitive_values_doesnt_hash_when_it_shouldnt():
    config = {"not_sensitive_key": "not sensitive key", "regular_key": "regular value"}
    hashed_config = hash_sensitive_values(config)
    assert hashed_config == config
