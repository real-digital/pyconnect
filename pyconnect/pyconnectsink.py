from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Dict, Any, List, Callable, TYPE_CHECKING, Optional

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaError

if TYPE_CHECKING:
    from confluent_kafka import Message


class Status(Enum):
    NOT_YET_RUNNING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


# Type definitions
Callback = Callable[..., Optional[Status]]

ERROR_MESSAGE = """Connector crashed when processing message nr {}.
Error Message and Trace:
{}
"""


class PyConnectSink(metaclass=ABCMeta):
    """ This is the base class that all custom sink connectors need to inherit

    There are a row of steps that a connector goes through:

    create a consumer configured to _NOT_ automatically commit any consumed messages back to kafka automatically
    make the consumer listen to topic X and poll for new messages
    for each incoming message X:
        check and handle API-dependent errors, edge cases etc.
        handle the message depending on the applications needs (write to file, to db, ...)
        this is forwarded to the specific sub-classes `handle_message()` method that it _must_ implement
        each [flush size] amount of messages that have been handled, do:
             manually commit the current consumer-groups offset back into kafka
             in order to signal that X messages have been delivered to the sink

    Avoiding to immediately acknowledge any incoming messages from kafka, and instead only committing when it's saved
    in the sink, makes sure that we can still provide at-least-once guarantees for the sink. Consider this case:
        If a message from kafka is consumed by the consumer group, and the connector dies/stops before this message
        is written into the actual sink, the connector might just assume it has been saved to the sink and not read it
        again once the connector restarts (since, per default, it will pick up where the consumer-group last fetched).
        By committing back the consumer-groups into kafka only when it's actually in the sink, we are able to
        fine-tune this behaviour so that we can re-try to send some messages in cases where the connector
        stopped/crashed after reading but before writing.

    However, it might still happen that messages are sent to the sink twice. Consider this case:
        The connector gets a message from kafka, writes it correctly into the sink, but crashes before it's able to
        commit this message back to kafka. When the connector is restarted, it will read the last message from kafka
        again and try to save it in the sink a second time.

    That means that a connector implementation shoud either (a) somehow check that the message has not yet been sent to
    the sink in earlier runs or (b) use only idempotent write operations to the sink so more-than-once-delivery
    is not a problem for the sink
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
        self.poll_timeout: int = config.get("poll_timeout", 0.5)
        self.consumer_options: Dict[str, str] = config.get("consumer_options", {})

        # The status can be changed from different events, like stopping from callbacks or crashing
        self._status: Status = Status.NOT_YET_RUNNING
        self.status_message = self.status.name

        self.processed: int = 0

        self._consumer: AvroConsumer = self._make_consumer()

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status: Status):
        """Property that makes sure that status changes trigger the corresponding hooks"""
        self._status = new_status
        if self._status == Status.STOPPED:
            self.stop()
        elif self._status == Status.CRASHED:
            self.crash()

    # public functions

    @abstractmethod
    def handle_message(self, msg: "Message") -> None:
        raise NotImplementedError("Need to implement and call this on a subclass")

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError("Need to implement and call this on a subclass")

    @abstractmethod
    def crash(self) -> None:
        raise NotImplementedError("Need to implement and call this on a subclass")

    def run(self) -> None:
        if not self.status == Status.NOT_YET_RUNNING:
            raise RuntimeError("Can not re-start a failed/stopped connector, need to re-create a Connect instance")

        self.status = Status.RUNNING
        self.status_message = self.status.name
        while self.status == Status.RUNNING:
            msg = self._consumer.poll(self.poll_timeout)
            self._handle_response(msg)

    # Optional hooks

    def on_message_handled(self, msg):
        pass

    def on_empty_poll(self):
        pass

    # internal functions with business logic

    def _make_consumer(self) -> AvroConsumer:
        config = {
            "bootstrap.servers": self.brokers,
            "group.id": self.connect_name,
            "schema.registry.url": self.schema_registry,
            "enable.auto.commit": False,  # We need to commit offsets manually once we"re sure it got saved to the sink
            "default.topic.config":  # We need this to start at the earliest offset instead of the latest when resuming
                {
                    "auto.offset.reset": "earliest"
                },
            **self.consumer_options
        }
        consumer = AvroConsumer(config)
        consumer.subscribe([self.topic])
        return consumer

    def _handle_message_internal(self, msg: "Message") -> None:
        try:
            self.handle_message(msg)
        except Exception as e:
            self.status = Status.CRASHED
            self.status_message = ERROR_MESSAGE.format(msg.offset(), str(e))
            return

        self.processed += 1
        self._maybe_flush_back(msg)

        self._run_callback(self.on_message_handled)

    def _maybe_flush_back(self, msg: "Message"):
        """Every time {self.flush_after}s messages have been written to the sink, write this info back into kafka

        TODO: Ensure flush also when there is a period of empty/no messages after some timeout
        """
        if self.processed % self.flush_after == 0:
            self._consumer.commit(message=msg, asynchronous=False)  # Blocking commit!

    def _handle_response(self, msg: "Message") -> None:
        """Handles errors of the raw message and then relays the message to `self._handle_message_internal()`"""
        # Both of those cases are simple "we have no further messages available" messages
        # EOF is returned the first time we hit the EOF, from then on we get None"s afer the poll()"s timeout runs out
        if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
            self._run_callback(self.on_empty_poll)
        elif msg.error():
            self.status = Status.CRASHED
            print("FATAL::", msg.error())
        else:
            self._handle_message_internal(msg)

    # internal utility functions
    def _run_callback(self, callback: Callback, *args: List[Any], **kwargs: Dict[Any, Any]) -> None:
        """Runs a given callback and handles the (optional) return value to (maybe) change the connector status"""
        new_status = callback(self, *args, **kwargs)
        if new_status is None:
            return
        if new_status not in Status:
            raise ValueError(f"Callback {str(callback)} must either return None or a valid Status")
        self.status = new_status

    def _debug_message(self, msg: "Message") -> Dict[str, Any]:
        return {a: getattr(msg, a)() for a in dir(msg) if not (a.startswith("__") or a.startswith("set"))}
