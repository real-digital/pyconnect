import hashlib
import re

from pyconnect.core import hide_sensitive_values


def test_hide_sensitive_values_hashes():
    config = {"sasl.password": "unhashed password", "regular_key": "regular value"}
    hashed_config = hide_sensitive_values(config)
    hash_pattern = r"\$PBKDF2-HMAC-(?P<algo>[^:]+):(?P<salt>[^:]+):(?P<iterations>\d+)\$(?P<hash>\w+)"
    groups = re.match(hash_pattern, hashed_config["sasl.password"]).groupdict()
    recomputed_hash = hashlib.pbkdf2_hmac(
        groups["algo"].lower(), b"unhashed password", bytes.fromhex(groups["salt"]), int(groups["iterations"])
    )

    assert recomputed_hash.hex() == groups["hash"]
    assert hashed_config["sasl.password"] != config["sasl.password"]
    assert hashed_config["regular_key"] == config["regular_key"]


def test_hide_sensitive_values_obfuscates():
    config = {"sasl.password": "unhashed password", "regular_key": "regular value"}
    hidden_config = hide_sensitive_values(config, hash_sensitive_values=False)

    assert hidden_config["sasl.password"] == "****"
    assert hidden_config["regular_key"] == config["regular_key"]


def test_hide_sensitive_values_doesnt_hash_when_it_shouldnt():
    config = {"not_sensitive_key": "not sensitive key", "regular_key": "regular value"}
    hashed_config = hide_sensitive_values(config)

    assert hashed_config == config
