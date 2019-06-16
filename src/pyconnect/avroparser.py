"""
This module offers functions which infer avro schemas from records.
"""
import json
from typing import Any, Dict

# TODO: refactor this code, especially the name 'avroparser' is misleading, should be something like 'schema_inference'
from confluent_kafka import avro as confluent_avro

RecordDict = Dict[str, Any]

SIMPLE_TYPE_MAP = {int: "long", float: "double", str: "string", bytes: "bytes", bytearray: "bytes", type(None): "null"}


def _parse_avro_field(name: str, element: Any, optional_primitives: bool) -> RecordDict:
    """
    Determine the type of a field and return its avro specification

    :param name: The name of the field.
    :param element: The content of the field.
    :param optional_primitives: Whether primitives within this field shall be defined as optional.
    :return: Inferred schema.
    """
    # TODO: _parse_avro_field shouldn't care about a field's name, this is the caller's responsibility

    elem_type = type(element)
    primitive_avro_type: Any = SIMPLE_TYPE_MAP.get(elem_type, None)

    # simple types
    if primitive_avro_type is not None:

        # Need to substitute "<type>" for ["null", "<type>"] when optional primitives are requested
        if optional_primitives is True and primitive_avro_type != "null":
            primitive_avro_type = ["null", primitive_avro_type]

        return {"name": name, "type": primitive_avro_type}

    if elem_type is list:
        # TODO FIXME: must be able to deal with records in lists as well!
        return {"type": "array", "items": "string"}

    # TODO we are assuming that all records eventually consist of primitive types
    # TODO maybe enforce this by to-json-from-json'ing all records first?

    # recursive records need a little different format - note that the "name" is duplicated in parent & child element
    return {"name": name, "type": {"name": name, "type": "record", **to_avro_fields(element, optional_primitives)}}


def to_avro_fields(record: RecordDict, optional_primitives: bool) -> RecordDict:
    """
    Resolves the avro type specification of a record which is a (possibly nested) structure of several fields.
    :param record: The record to infer the schema from.
    :param optional_primitives: Whether primitives within this record shall be defined as optional.
    :return: Inferred schema.
    """
    data = []
    for key, value in record.items():
        data.append(_parse_avro_field(key, value, optional_primitives))
    return {"fields": data}


def create_schema_from_record(
    name: str, record: Any, namespace: str = None, optional_primitives: bool = False
) -> RecordDict:
    """
    Infers the avro schema from a record which may be a simple primitive type or a structure.
    :param name: The root name for this schema.
    :param record: The record to infer the schema from.
    :param namespace: Optional, gives a name to the schema, could be for example the name of the object (Table,
                      File ...) where the records originate from.
    :param optional_primitives: Whether primitives within this schema shall be defined as optional.
    :return: Inferred schema.
    """
    # TODO also allow a way to make each primitive type optional

    if isinstance(record, dict):
        # for top-level elements, the name might be either "key" or "value"
        template = {"name": name, "type": "record", **to_avro_fields(record, optional_primitives)}
    else:
        template = _parse_avro_field(name, record, optional_primitives)

    # This is optional - should be specified for root-level schemata but not necessary for recursive records
    if namespace is not None:
        template["namespace"] = namespace

    return template


def to_key_schema(record: Any):
    """
    Utility function that turns a record into a schema with name 'key' that is readily usable by
    :meth:`confluent_kafka.avro.AvroProducer.produce`.

    :param record: The key record to infer the schema from.
    :return: Key schema.
    """
    return confluent_avro.loads(json.dumps(create_schema_from_record("key", record)))


def to_value_schema(record: Any):
    """
    Utility function that turns a record into a schema with name 'value' that is readily usable by
    :meth:`confluent_kafka.avro.AvroProducer.produce`.

    :param record: The value record to infer the schema from.
    :return: Value schema.
    """
    return confluent_avro.loads(json.dumps(create_schema_from_record("value", record)))
