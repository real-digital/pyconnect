from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Tuple, Dict, Any, Optional

from pyconnect.config import SinkConfig
from pyconnect.core import Status, BaseConnector

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaException
from confluent_kafka import Message, TopicPartition

import logging
logger = logging.getLogger(__name__)


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


class PyConnectSink(BaseConnector, metaclass=ABCMeta):
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
        self._offsets: Dict[Tuple[str, int], TopicPartition] = {}

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

    def close(self):
        try:
            self._consumer.close()
        except RuntimeError:
            # closing a closed consumer should not raise anything
            pass

    # Optional hooks

    def on_error_received(self, msg):
        pass

    def on_no_message_received(self):
        pass

    def need_flush(self):
        return True

    def on_final_flush(self):
        status = self.on_flush()
        # maybe on_flush() returned RUNNING
        # return None in that case, in order not to overwrite actual status
        if status == Status.RUNNING:
            return None
        return status

    # Hook wrappers

    def _on_message_received(self, msg: Message):
        self._update_offset_from_message(msg)
        self._safe_call_and_set_status(self.on_message_received, msg)

    def _on_flush(self):
        self._safe_call_and_set_status(self.on_flush)

    def _on_final_flush(self):
        self._safe_call_and_set_status(self.on_final_flush)

    def _on_error_received(self, msg: Message):
        self._safe_call_and_set_status(self.on_error_received, msg)

    def _on_no_message_received(self):
        self._safe_call_and_set_status(self.on_no_message_received)

    # internal functions with business logic

    def _update_offset_from_message(self, msg: Message):
        topic_partition = self._msg_to_topic_partition(msg)
        key = (topic_partition.topic, topic_partition.partition)
        self._offsets[key] = topic_partition

    def _msg_to_topic_partition(self, msg: Message) -> TopicPartition:
        # TopicPartition may contain an offset so it can be used with
        # consumer.commit, we need to add 1 so the consumer will continue
        # on the NEXT message when it resumes
        return TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)

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

    def _after_run_loop(self):
        try:
            if self._status == Status.STOPPED:
                self._on_final_flush()

                if self._status == Status.STOPPED:
                    self._commit()

            self._on_shutdown()
        finally:
            self.close()

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
                self._commit()

    def _commit(self) -> None:
        offsets = list(self._offsets.values())
        self._consumer.commit(offsets=offsets)

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
        return consumer
