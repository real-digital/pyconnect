import json

from confluent_kafka.avro import loads

from pyconnect.avroparser import create_schema_from_record

data = {
    "fint": 1,
    "fnull": None,
    "ffloat": 1.0,
    "flong": 10000000000000000000000000000000000,
    "fdouble": 2.0,
    "fstring": "hi there",
    "fobj": {"fobjint": 2, "fobjstr": "hello there"},
}

schema = {
    "name": "all_field",
    "namespace": "ba.nanas",
    "type": "record",
    "fields": [
        {"name": "fint", "type": "long"},
        {"name": "fnull", "type": "null"},
        {"name": "ffloat", "type": "double"},
        {"name": "flong", "type": "long"},
        {"name": "fdouble", "type": "double"},
        {"name": "fstring", "type": "string"},
        {
            "name": "fobj",
            "type": {
                "name": "fobj",
                "fields": [{"name": "fobjint", "type": "long"}, {"name": "fobjstr", "type": "string"}],
                "type": "record",
            },
        },
    ],
}

schema_optional = {
    "name": "all_field",
    "namespace": "ba.nanas",
    "type": "record",
    "fields": [
        {"name": "fint", "type": ["null", "long"]},
        {"name": "fnull", "type": "null"},
        {"name": "ffloat", "type": ["null", "double"]},
        {"name": "flong", "type": ["null", "long"]},
        {"name": "fdouble", "type": ["null", "double"]},
        {"name": "fstring", "type": ["null", "string"]},
        {
            "name": "fobj",
            "type": {
                "name": "fobj",
                "fields": [
                    {"name": "fobjint", "type": ["null", "long"]},
                    {"name": "fobjstr", "type": ["null", "string"]},
                ],
                "type": "record",
            },
        },
    ],
}


def test_avro_schema_generation():
    # Sanity check - this should not throw
    loads(json.dumps(schema))
    generated = create_schema_from_record("all_field", data, namespace="ba.nanas")
    assert schema == generated, f"Generated schema does not match!"


def test_schema_generation_from_primitive():
    record = "asdf"
    _generated = create_schema_from_record("key", record)
    _schema = {"name": "key", "type": "string"}

    assert _generated == _schema


def test_avro_schema_generation_optional():
    # Sanity check - this should not throw
    loads(json.dumps(schema_optional))
    generated = create_schema_from_record("all_field", data, namespace="ba.nanas", optional_primitives=True)

    assert schema_optional == generated, f"Generated schema does not match!"
