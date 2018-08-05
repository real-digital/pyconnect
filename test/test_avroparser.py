from pyconnect.avroparser import create_schema_from_record
import json
from confluent_kafka.avro import loads

data = {
    "fint": 1,
    "fnull": None,
    "ffloat": 1.0,
    "flong": 10000000000000000000000000000000000,
    "fdouble": 2.0,
    "fstring": "hi there",
    "fobj": {
        "fobjint": 2,
        "fobjstr": "hello there"
    }
}

schema = {
    "name": "all_field",
    "namespace": "ba.nanas",
    "type": "record",
    "fields": [
        {
            "name": "fint",
            "type": "long"
        },
        {
            "name": "fnull",
            "type": "null"
        },
        {
            "name": "ffloat",
            "type": "double"
        },
        {
            "name": "flong",
            "type": "long"
        },
        {
            "name": "fdouble",
            "type": "double"
        },
        {
            "name": "fstring",
            "type": "string"
        },
        {
            "name": "fobj",
            "type": {
                "name": "fobj",
                "fields": [
                    {
                        "name": "fobjint",
                        "type": "long"
                    },
                    {
                        "name": "fobjstr",
                        "type": "string"
                    }
                ],
                "type": "record"
            }
        }
    ]
}

def test_avro_schema_generation():
    # Sanity check - this should not throw
    loads(json.dumps(schema))
    generated = create_schema_from_record("all_field", data, namespace="ba.nanas")
    _generated = json.dumps(generated, sort_keys=True)
    _schema = json.dumps(schema, sort_keys=True)
    assert _schema == _generated, f"Generated schema does not match!"


