from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Dict, Any, List, Callable, Optional

from pyconnect.config import SinkConfig

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaException
from confluent_kafka import Message

import logging
logger = logging.getLogger(__name__)


class Status(Enum):
    NOT_YET_RUNNING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


class MessageType(Enum):
    STANDARD = 0
    NO_MESSAGE = 1
    ERROR = 2


def determine_message_type(msg: Optional[Message]) -> MessageType:
    if msg is None:
        return MessageType.NO_MESSAGE
    if msg.error() is not None:
        return MessageType.ERROR
    return MessageType.STANDARD


class PyConnectSink(metaclass=ABCMeta):
    """ This is the base class that all custom sink connectors need to inherit

    There are a row of steps that a connector goes through:

    create a consumer configured to _NOT_ automatically commit any consumed
    messages back to kafka automatically
    make the consumer listen to topic X and poll for new messages
    for each incoming message X:
        check and handle API-dependent errors, edge cases etc.
        handle the message depending on the applications needs
        (write to file, to db, ...)
        this is forwarded to the specific sub-classes `handle_message()`
        method that it _must_ implement
        each [flush size] amount of messages that have been handled, do:
             manually commit the current consumer-groups offset back into kafka
             in order to signal that X messages have been delivered to the sink

    Avoiding to immediately acknowledge any incoming messages from kafka, and
    instead only committing when it's saved in the sink, makes sure that we can
    still provide at-least-once guarantees for the sink. Consider this case:
        If a message from kafka is consumed by the consumer group, and the
        connector dies/stops before this message is written into the actual
        sink, the connector might just assume it has been saved to the sink and
        not read it again once the connector restarts (since, per default, it
        will pick up where the consumer-group last fetched).  By committing
        back the consumer-groups into kafka only when it's actually in the
        sink, we are able to fine-tune this behaviour so that we can re-try to
        send some messages in cases where the connector stopped/crashed after
        reading but before writing.

    However, it might still happen that messages are sent to the sink twice.
    Consider this case:
        The connector gets a message from kafka, writes it correctly into the
        sink, but crashes before it's able to commit this message back to
        kafka. When the connector is restarted, it will read the last message
        from kafka again and try to save it in the sink a second time.

    That means that a connector implementation shoud either (a) somehow check
    that the message has not yet been sent to the sink in earlier runs or (b)
    use only idempotent write operations to the sink so more-than-once-delivery
    is not a problem for the sink
    """

    def __init__(self, config: SinkConfig) -> None:
        self.config: SinkConfig = config

        # The status can be changed from different events, like stopping from
        # callbacks or crashing
        self._status: Status = Status.NOT_YET_RUNNING
        self._status_info: Any = None

        self._consumer: AvroConsumer = self._make_consumer()
        self.last_message: Message = None

    @property
    def is_running(self):
        return self._status == Status.RUNNING

    @property
    def status_info(self):
        return self._status_info

    @property
    def status(self):
        return self._status

    # public functions
    @abstractmethod
    def on_flush(self) -> None:
        raise NotImplementedError("Need to implement and call "
                                  "this on a subclass")

    @abstractmethod
    def on_message_received(self, msg: Message) -> None:
        raise NotImplementedError("Need to implement and call "
                                  "this on a subclass")

    def run(self) -> None:
        self._before_run_loop()
        self._run_loop()
        self._after_run_loop()

    # Optional hooks

    def on_crash(self):
        pass

    def on_shutdown(self):
        pass

    def on_startup(self):
        pass

    def on_error_received(self, msg):
        pass

    def on_no_message_received(self):
        pass

    def need_flush(self):
        return True

    # Hook wrappers

    def _on_message_received(self, msg: Message):
        self._safe_call_and_set_status(self.on_message_received, msg)

    def _on_flush(self):
        self._safe_call_and_set_status(self.on_flush)

    def _on_crash(self):
        self._safe_call_and_set_status(self.on_crash)

    def _on_shutdown(self):
        # connector cannot recover during on_shutdown!
        self.on_shutdown()

    def _on_startup(self):
        self._safe_call_and_set_status(self.on_startup)

    def _on_error_received(self, msg: Message):
        self._safe_call_and_set_status(self.on_error_received, msg)

    def _on_no_message_received(self):
        self._safe_call_and_set_status(self.on_no_message_received)

    # internal functions with business logic
    def _before_run_loop(self):
        pass
        if not self.status == Status.NOT_YET_RUNNING:
            raise RuntimeError('Can not re-start a failed/stopped connector, '
                               'need to re-create a Connect instance')

        self._status = Status.RUNNING

        self._on_startup()

    def _run_loop(self):
        while self.is_running:
            self._run_once()

    def _run_once(self) -> None:
        try:
            self.last_message = None
            self._status_info = None
            msg = self._consumer.poll(self.config.poll_timeout)
            self.last_message = msg
            self._call_right_handler_for_message(msg)
            self._flush_if_needed_and_commit()
        except KafkaException as e:
            self._handle_kafka_exception(e)
        except Exception as e:
            self._handle_general_exception(e)
        if self.status == Status.CRASHED:
            self._on_crash()

    def _handle_kafka_exception(self, e: KafkaException) -> None:
        logger.exception('Kafka internal exception!')
        self._status = Status.CRASHED
        self._status_info = e

    def _handle_general_exception(self, e: Exception):
        logger.exception('Connector crashed!')
        self._status = Status.CRASHED
        self._status_info = e

    def _after_run_loop(self):
        try:
            self.on_shutdown()
        finally:
            self._consumer.close()

    def _call_right_handler_for_message(self, msg: Message) -> None:
        msg_type = determine_message_type(msg)
        if msg_type == MessageType.STANDARD:
            self._on_message_received(msg)
        elif msg_type == MessageType.NO_MESSAGE:
            self._on_no_message_received()
        elif msg_type == MessageType.ERROR:
            self._on_error_received(msg)

    def _flush_if_needed_and_commit(self) -> None:
        if self.is_running and self.need_flush():
            self._on_flush()
            # only commit if status after flushing is still running
            if self.is_running:
                self._consumer.commit()

    def _safe_call_and_set_status(self, callback: Callable,
                                  *args: List[Any], **kwargs: Dict[Any, Any]):
        try:
            new_status = callback(*args, **kwargs)
        except Exception as e:
            logger.exception(f'Callback {callback} raised an Exception!')
            self._status = Status.CRASHED

        if new_status is None:
            return
        elif isinstance(new_status, Status):
            self._status = new_status
        else:
            raise RuntimeError(f'Callback {callback} needs to return Status '
                               f'but returned {type(new_status)}')

    def _make_consumer(self) -> AvroConsumer:
        config = {
            "bootstrap.servers": ','.join(self.config.bootstrap_servers),
            "group.id": self.config.group_id,
            "schema.registry.url": self.config.schema_registry,

            # We need to commit offsets manually once we"re sure it got saved
            # to the sink
            "enable.auto.commit": False,

            # One error less to worry about
            "enable.partition.eof": False,

            # We need this to start at the last committed offset instead of the
            # latest when subscribing for the first time
            "default.topic.config": {
                "auto.offset.reset": "earliest"
            },
            **self.config.consumer_options
        }
        consumer = AvroConsumer(config)
        consumer.subscribe(self.config.topics)
        # consumer.resume()
        return consumer
