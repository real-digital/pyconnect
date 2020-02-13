from pyconnect.core import hide_plaintext_sensitive_values


def test_hide_plaintext_sensitive_values_hashes():
    config = {"sasl.password": "unhashed password", "regular_key": "regular value"}
    hashed_config = hide_plaintext_sensitive_values(config)
    assert hashed_config["sasl.password"] != config["sasl.password"]
    assert "PBKDF2-HMAC-" in hashed_config["sasl.password"]
    assert hashed_config["regular_key"] == config["regular_key"]


def test_hide_plaintext_sensitive_values_obfuscates():
    config = {"sasl.password": "unhashed password", "regular_key": "regular value"}
    hashed_config = hide_plaintext_sensitive_values(config, hash_passwords=False)
    assert hashed_config["sasl.password"] == "****"
    assert hashed_config["regular_key"] == config["regular_key"]


def test_hide_plaintext_sensitive_values_doesnt_hash_when_it_shouldnt():
    config = {"not_sensitive_key": "not sensitive key", "regular_key": "regular value"}
    hashed_config = hide_plaintext_sensitive_values(config)
    assert hashed_config == config
