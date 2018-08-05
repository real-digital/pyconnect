from avro.schema import Parse

SIMPLE_TYPE_MAP = {
    int: "long",
    float: "double",
    str: "string",
    bytes: "bytes",
    bytearray: "bytes",
    type(None): "null"
}


def _parse_avro_field(name, element):

    elem_type = type(element)

    # simple types
    avro_type = SIMPLE_TYPE_MAP.get(elem_type, None)
    if avro_type is not None:
        return {
            "name": name,
            "type": avro_type
        }

    if elem_type is list:
        # TODO FIXME: must be able to deal with records in lists as well!
        return {"type": "array", "items": "string"}

    # TODO we are assuming that all records eventually consist of primitive types
    # TODO maybe enforce this by to-json-from-json'ing all records first?

    # recursive records need a little different format
    return {
        "name": name,
        "type": {
            "name": name,
            "type": "record",
            **to_avro_fields(element)
        }
    }


def to_avro_fields(record):
    data = []
    for key, value in record.items():
        data.append(_parse_avro_field(key, value))
    return {
        "fields": data
    }


def create_schema_from_record(name, record, namespace=None):
    # TODO also allow a way to make each primitive type optional

    # for top-level elements, the name might be either "key" or "value"
    template = {
        "name": name,
        "type": "record",
        **to_avro_fields(record)
    }

    # This is optional - should be specified for root-level schemata but not necessary for recursive records
    if namespace is not None:
        template["namespace"] = namespace

    return template

def parse_avro_schema_from_json_schema(json_schema):
    return Parse(str(json_schema))

def infer_schema_from_record(name, record, namespace=None):
    schema = create_schema_from_record(name, record, namespace=namespace)
    return parse_avro_schema_from_json_schema()