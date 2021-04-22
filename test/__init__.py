from confluent_kafka.schema_registry import Schema, avro


def _schema_loads(schema_str):
    """
    Instantiates a Schema instance from a declaration string
    Args:
        schema_str (str): Avro Schema declaration.
    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas
    Returns:
        Schema: Schema instance
    """

    schema_str = schema_str.strip()  # canonical form primitive declarations are not supported
    if schema_str[0] != "{":
        if schema_str[0] != '"':
            schema_str = '{"type":"' + schema_str + '"}'
        else:
            schema_str = '{"type":' + schema_str + "}"

    return Schema(schema_str, schema_type="AVRO")


# TODO temporary fix until this issue will be fixed
# https://github.com/confluentinc/confluent-kafka-python/issues/989
avro._schema_loads = _schema_loads
