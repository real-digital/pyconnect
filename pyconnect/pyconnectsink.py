import json
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Dict, Any, List, Callable, TYPE_CHECKING, Union, Optional, Tuple

from avro.schema import Schema
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro import loads
from confluent_kafka.cimpl import KafkaError

if TYPE_CHECKING:
    from confluent_kafka import Message


class Status(Enum):
    NOT_YET_RUNNING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


def noop(*args, **kwargs):
    pass


# Type definitions
Callback = Callable[..., Optional[Status]]


class PyConnectSink(metaclass=ABCMeta):
    """

    There are a row of steps that a connector goes through:

    listen to topic X and poll for new messages
    for each incoming message X:
        the consumer-group's offset will automatically be incremented to signal that X has been received by the group
        handle it depending on the applications needs (write to file, to db, ...)
        if [flush size] amount of messages that have been handled:
             manually write the current offset into a special kafka topic
             to signal that X messages have been delivered to the sink

    This "2-phase saving of offsets" makes sure that we can still provide at-least-once guarantees for the sink:
        If a message from kafka is consumed by the consumer group, and the connector dies/stops before this
        message is written into the actual sink, the connector might just assume it has been saved to the sink and not
        read it again once the connector restarts (since it will pick up where the consumer-group stopped).
        By committing back the "offset we've actually managed to write to the sink" into kafka, we are able to
        fine-tune this behaviour so that we can reset the consumer-group offset and re-try to send some messages in
        cases where the connector stopped/crashed after reading but before writing.

    This, however, makes the "start connector" part of the code more complex:
        We need to reset the consumer-group offsets and seek to the specific point Y in the topic where we're sure
        that message Y has actually been delivered to the sink, and then continue with message Y+1.
        In that sense, the actual consumer-group offsets only indicates the approximate "area" of the topic,
        but it doesn't really help us in achieving "at least once" guarantees.

    TODO: might not be necessary: maybe we can use https://docs.confluent.io/current/clients/consumer.html#configuration
    """

    def __init__(self, **config: Dict[str, Any]) -> None:

        self.connect_name: str = config["connect_name"]
        self.brokers: str = config["brokers"]
        self.topic: str = config["topic"]
        self.schema_registry: str = config["schema_registry"]
        self.flush_after: int = config["flush_after"]
        assert self.flush_after > 0, "Setting `flush_after` must be higher than 0!"

        # TODO make sure this is a valid topic name
        self.offset_topic_name = config.get("offset_topic_name", f"_pyconnect_offsets")
        self.on_message_handled: Callable = config.get("on_message_handled", noop)
        self.on_empty_poll: Callable = config.get("on_empty_poll", noop)
        self.poll_timeout: int = config.get("poll_timeout", 0.5)

        self.status: Status = Status.NOT_YET_RUNNING
        self.processed: int = 0
        self.current_consumed_offset = None
        self.current_produced_offset = None

        self._consumer: AvroConsumer = None
        self._init_consumer()

        self._producer: AvroProducer = None
        self._init_producer()

    # various initializers and configs

    def _init_consumer(self) -> None:
        self._consumer = AvroConsumer(self._get_consumer_config())
        self._consumer.subscribe([self.topic])

    def _init_producer(self) -> None:
        key_schema, value_schema = self._get_connect_offset_schema()
        self._producer = AvroProducer(
            self._get_producer_config(),
            default_key_schema=key_schema,
            default_value_schema=value_schema
        )

    def _get_producer_config(self) -> Dict[str, Union[str, Dict[str, str]]]:
        return {
            "bootstrap.servers": self.brokers,
            "schema.registry.url": self.schema_registry
        }

    def _get_consumer_config(self) -> Dict[str, Union[str, Dict[str, str]]]:
        return {
            "bootstrap.servers": self.brokers,
            "group.id": self.connect_name,
            "schema.registry.url": self.schema_registry,
            'auto.offset.reset': 'smallest',
            'default.topic.config':
                {
                    'auto.offset.reset': 'smallest'
                }
        }

    def _get_connect_offset_schema(self) -> Tuple[Schema, Schema]:
        key_schema = {
            "name": "key",
            "namespace": f"pyconnect_offsets.{self.connect_name}",
            "type": "record",
            "fields": [
                {
                    "name": "connector_name",
                    "type": "string"
                }
            ]
        }
        value_schema = {
            "name": "value",
            "namespace": f"pyconnect_offsets.{self.connect_name}",
            "type": "record",
            "fields": [
                {
                    "name": "offset",
                    "type": "long"
                }
            ]
        }

        return loads(json.dumps(key_schema)), loads(json.dumps(value_schema))

    # public functions

    @abstractmethod
    def handle_message(self, msg: "Message") -> None:
        raise NotImplementedError("Need to implement and call this on a subclass")

    def stop(self) -> None:
        self.status = Status.STOPPED

    def run(self) -> None:
        self.status = Status.RUNNING
        while self.status == Status.RUNNING:
            msg = self._consumer.poll(self.poll_timeout)
            self._handle_response(msg)

    # internal functions with business logic

    def _handle_message_internal(self, msg: "Message") -> None:
        self.current_consumed_offset = msg.offset()
        self.handle_message(msg)
        self.processed += 1
        if self.processed % self.flush_after == 0:
            self._flush_back(msg)
            self.current_produced_offset = msg.offset()
        self._run_callback(self.on_message_handled)

    def _run_callback(self, callback: Callback, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        """Runs a given callback and handles the (optional) return value to (maybe) change the connector status"""
        new_status = callback(self, *args, **kwargs)
        if new_status is None:
            return
        if new_status not in Status:
            raise ValueError(f"Callback {str(callback)} must either return None or a valid Status")
        self.status = new_status

    def _flush_back(self, msg: "Message"):
        """Called every time {self.flush_after}s messages have been written to the sink to write this info into kafka

        """
        # TODO ensure topic exists and has only 1 partition in a separate function
        self._producer.produce(
            topic=self.offset_topic_name,
            key={"connector_name": self.connect_name},
            value={"offset": msg.offset()}
        )

        pass  # TODO Write test for that first

    def _handle_response(self, msg: "Message") -> None:
        """Handles errors of the raw message and then relays the message to `self._handle_message_internal()`"""
        # Both of those cases are simple "we have no further messages available" messages
        # EOF is returned the first time we hit the EOF, from then on we get None's afer the poll()'s timeout runs out
        if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
            self._run_callback(self.on_empty_poll)
        elif msg.error():
            self.status = Status.CRASHED
            print("FATAL::", msg.error())
        else:
            self._handle_message_internal(msg)

    # internal utility functions

    def _debug_message(self, msg: "Message") -> Dict[str, Any]:
        return {a: getattr(msg, a)() for a in dir(msg) if not (a.startswith("__") or a.startswith("set"))}
