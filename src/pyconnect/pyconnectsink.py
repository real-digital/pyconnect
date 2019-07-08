import logging
import struct
import warnings
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Tuple

from confluent_kafka import Message, TopicPartition
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaError

from .config import SinkConfig
from .core import BaseConnector, Status, message_repr

logger = logging.getLogger(__name__)


class MessageType(Enum):
    """
    This enum classifies the response of :meth:`confluent_kafka.Consumer.poll`.

    +============+==========================================================================+
    | Name       | Description                                                              |
    +============+==========================================================================+
    | STANDARD   | A normal message with proper content.                                    |
    +------------+--------------------------------------------------------------------------+
    | NO_MESSAGE | Either no message or EOF.                                                |
    +------------+--------------------------------------------------------------------------+
    | ERROR      | The message contains some event or error data, check its `error` method. |
    +------------+--------------------------------------------------------------------------+
    | EOF        | The message indicates the end of a partition.                            |
    +------------+--------------------------------------------------------------------------+
    """

    STANDARD = 0
    NO_MESSAGE = 1
    ERROR = 2
    EOF = 3


def determine_message_type(msg: Optional[Message]) -> MessageType:
    """
    Takes a response from :meth:`confluent_kafka.Consumer.poll` and classifies it according to
    :class:`pyconnect.pyconnectsink.MessageType`.

    :param msg: The message to classify.
    :return: The classification.
    """
    if msg is None:
        return MessageType.NO_MESSAGE
    if msg.error() is not None:
        if msg.error().code() == KafkaError._PARTITION_EOF:
            return MessageType.EOF
        else:
            return MessageType.ERROR
    return MessageType.STANDARD


def msg_to_topic_partition(msg: Message) -> TopicPartition:
    """
    Takes a :class:`confluent_kafka.Message` and reads its attributes in order to create a
    :class:`confluent_kafka.TopicPartition`.

    :param msg: Message to read partition and offset information from.
    :return: The extracted partition and offset.
    """
    return TopicPartition(msg.topic(), msg.partition(), msg.offset())


class RichAvroConsumer(AvroConsumer):
    """
    Kafka Consumer client which does avro schema decoding of messages.
    Handles message deserialization.

    Constructor takes below parameters

    :param dict config: Config parameters containing url for schema registry (``schema.registry.url``)
                        and the standard Kafka client configuration (``bootstrap.servers`` et.al).
    """

    def __init__(self, config, schema_registry=None):

        super().__init__(config, schema_registry=schema_registry)
        self._current_key_schema_id = None
        self._current_value_schema_id = None

    @staticmethod
    def extract_schema_id(message: bytes) -> int:
        _, schema_id = struct.unpack(">bI", message[:5])
        return schema_id

    @property
    def current_key_schema_id(self) -> int:
        return self._current_key_schema_id

    @property
    def current_value_schema_id(self) -> int:
        return self._current_value_schema_id

    def poll(self, timeout=None):
        """
        This is an overriden method from confluent_kafka.Consumer class. This handles message
        deserialization using avro schema

        :param float timeout: Poll timeout in seconds (default: indefinite)
        :returns: message object with deserialized key and value as dict objects
        :rtype: Message
        """
        if timeout is None:
            timeout = -1

        # Call grandparent's poll method to skip AvroConsumer's message conversion
        message = super(AvroConsumer, self).poll(timeout)
        if message is None:
            return None
        if not message.value() and not message.key():
            return message
        if not message.error():
            if message.value() is not None:
                self._current_value_schema_id = self.extract_schema_id(message.value())
                decoded_value = self._serializer.decode_message(message.value())
                message.set_value(decoded_value)
            if message.key() is not None:
                self._current_key_schema_id = self.extract_schema_id(message.key())
                decoded_key = self._serializer.decode_message(message.key())
                message.set_key(decoded_key)

        return message


class PyConnectSink(BaseConnector, metaclass=ABCMeta):
    """
    This class offers base functionality for all sink connectors. All sink connectors have to inherit from this class
    and implement its abstract methods.
    There are also a few optional callbacks that can be overridden if a sink implementation needs them.
    For an exemplary implementation of this class have a look at :class:`test.utils.PyConnectTestSink`.
    """

    def __init__(self, config: SinkConfig) -> None:
        super().__init__()
        self.config = config

        self.current_message: Optional[Message] = None
        self.__offsets: Dict[Tuple[str, int], TopicPartition] = {}
        self.__eof_reached: Dict[Tuple[str, int], bool] = {}
        self._consumer: RichAvroConsumer = self._make_consumer()

    def _make_consumer(self) -> RichAvroConsumer:
        config = {
            "bootstrap.servers": ",".join(self.config["bootstrap_servers"]),
            "group.id": self.config["group_id"],
            "schema.registry.url": self.config["schema_registry"],
            # We need to commit offsets manually once we're sure it got saved
            # to the sink
            "enable.auto.commit": False,
            # We want to keep track of EOFs
            "enable.partition.eof": True,
            # We need this to start at the last committed offset instead of the
            # latest when subscribing for the first time
            "default.topic.config": {"auto.offset.reset": "earliest"},
            **self.config["kafka_opts"],
        }
        consumer = RichAvroConsumer(config)
        logger.info(f"AvroConsumer created with config: {config}")
        # noinspection PyArgumentList
        consumer.subscribe(self.config["topics"], on_assign=self._on_assign, on_revoke=self._on_revoke)
        return consumer

    def _on_assign(self, _, partitions: List[TopicPartition]) -> None:
        """
        Handler for topic assignment. When the consumer is assigned to a new topic partition, either during initial
        subscription or later rebalance, then this function is called and will set the EOF reached flag for all
        assigned partitions to `False`.
        This callback is registered automatically on topic subscription.
        """
        logger.info(f"Assigned to partitions: {partitions}")
        for partition in partitions:
            self.__eof_reached[(partition.topic, partition.partition)] = False

    def _on_revoke(self, _, partitions: List[TopicPartition]):
        """
        Handler for revoked topic partitions. When the consumer is revoked from topic partitions during rebalance,
        then this function is called. It will commit all offsets already handled and then delete the EOF-reached flag
        and offsets for all revoked partitions.
        This callback is registered automatically on topic subscription.
        """

        # self.close will trigger this via self._consumer.close() which entails topic revocation
        # however, we have the after_run_loop method to deal with flushing when we're finished and we certainly don't
        # want to flush when we crashed, so don't do this
        if self._status == Status.CRASHED:
            logger.info(f"Revoked from partitions: {partitions}, handling skipped due to crash")
            return
        logger.info(f"Revoked from partitions: {partitions}, triggering a flush")

        self._on_flush()
        for partition in partitions:
            topic_partition = (partition.topic, partition.partition)
            self.__eof_reached.pop(topic_partition, None)
            self.__offsets.pop(topic_partition, None)

    @property
    def all_partitions_at_eof(self):
        return all(self.__eof_reached.values())

    @property
    def has_partition_assignments(self):
        return len(self._consumer.assignment()) > 0

    @property
    def last_message(self):
        # TODO: when bumping to next major release, remove this property
        warnings.warn(
            "'last_message' will be permanently renamed to 'current_message' in next major release", DeprecationWarning
        )
        return self.current_message

    def _run_once(self) -> None:
        try:
            self.current_message = None
            self._status_info = None
            msg = self._consumer.poll(self.config["poll_timeout"])
            self.current_message = msg
            self._flush_if_needed()
            self._call_right_handler_for_message(msg)
        except Exception as e:
            self._handle_exception(e)
        if self.status == Status.CRASHED:
            self._on_crash_during_run()

    def _call_right_handler_for_message(self, msg: Message) -> None:
        """
        Calls the right handler according to the message's type. The message is meant to be the return value given by
        :meth:`confluent_kafka.Consumer.poll`.
        """
        if msg is not None:
            logger.debug(f"Message received: {message_repr(msg)}")
        else:
            logger.debug("Message received: None")

        msg_type = determine_message_type(msg)
        if msg_type == MessageType.STANDARD:
            self._on_message_received(msg)
        elif msg_type == MessageType.NO_MESSAGE:
            self._on_no_message_received()
        elif msg_type == MessageType.ERROR:
            self._on_error_received(msg)
        elif msg_type == MessageType.EOF:
            self._on_eof_received(msg)

    def _on_message_received(self, msg: Message):
        self.__eof_reached[(msg.topic(), msg.partition())] = False
        self._unsafe_call_and_set_status(self.on_message_received, msg)
        self._update_offset_from_message(msg)

    def _update_offset_from_message(self, msg: Message):
        """
        Takes a message and updates the cached offset information with it so offsets are up to date when
        we commit them.
        """
        topic_partition = msg_to_topic_partition(msg)
        topic_partition.offset += 1
        key = (topic_partition.topic, topic_partition.partition)
        logger.debug(f"Updating offset: {topic_partition}")
        self.__offsets[key] = topic_partition

    @abstractmethod
    def on_message_received(self, msg: Message) -> Optional[Status]:
        """
        This callback is called whenever the sink's consumer has consumed a proper message. The callback is called
        with that very message and is supposed to buffer or persist or do whatever it needs to with it.

        :param msg: The last received message.
        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        raise NotImplementedError("Need to implement and call this on a subclass")

    def _on_no_message_received(self):
        self._unsafe_call_and_set_status(self.on_no_message_received)

    def _on_eof_received(self, msg: Message):
        key = (msg.topic(), msg.partition())
        self.__eof_reached[key] = True

        # when the sink has been restarted but is already at the end of the topic, this is how we
        # get the current offset. We need to keep committing this so the offsets in kafka won't get deleted.
        topic_partition = msg_to_topic_partition(msg)
        key = (topic_partition.topic, topic_partition.partition)
        logger.debug(f"Updating offset: {topic_partition}")
        self.__offsets[key] = topic_partition

        self._unsafe_call_and_set_status(self.on_eof_received, msg)

    def on_eof_received(self, msg: Message):
        """
        This callback is called whenever the sink's consumer has hit the end of a partition.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def on_no_message_received(self):
        """
        This callback is called whenever the sink's consumer has not received a message.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def _on_error_received(self, msg: Message):
        self._unsafe_call_and_set_status(self.on_error_received, msg)

    def on_error_received(self, msg):
        """
        This callback is called whenever the sink's consumer has consumed a message that contained a
        :class:`confluent_kafka.KafkaError`. The callback is called with that very message and may take whatever action
        it seems fit (i.e. set Status to CRASHED or raise an exception).
        Default behaviour is to do nothing since most errors are already handled by the kafka client library.

        :param msg: The last received message.
        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def _flush_if_needed(self) -> None:
        if self.is_running and self.need_flush():
            self._on_flush()

    def need_flush(self):
        """
        Called regularly at the start of each run loop cycle after
        :attr:`pyconnect.pyconnectsink.PyConnectSink.current_message` has been set to the newly arrived message.
        This function determines whether :meth:`pyconnect.pyconnectsink.PyConnectSink.on_flush` needs to be run
        before the message handler is called.

        Default behaviour is to return True all the time so every message is flushed and its offset committed.

        :return: Boolean indicating whether it's time to flush or not.
        """
        # TODO use config parameter 'flush_interval'
        return True

    def _on_flush(self):
        self._unsafe_call_and_set_status(self.on_flush)
        if self._status != Status.CRASHED:
            self._unsafe_call_and_set_status(self._commit)
        else:
            logger.info("Commit skipped due to crash")

    @abstractmethod
    def on_flush(self) -> Optional[Status]:
        """
        This callback is called whenever the sink is supposed to flush all messages it has consumed so far.
        There are two situations in which this is the case:

            1. At the start of a run loop cycle, when :meth:`pyconnect.pyconnectsink.PyConnectSink.need_flush` returns
               `True`.

            2. After the run loop, during :meth:`pyconnect.pyconnectsink.PyConnectSink.on_shutdown` if it wasn't
               overridden.

        Unless it raises an Exception or returns :obj:`pyconnect.core.Status.CRASHED` the current offsets for this
        sink's consumer will be committed after this callback returns.

        A consumer implementation might choose to not take any action if its
        :meth:`pyconnect.pyconnectsink.PyConnectSink.on_message_received` method is already persisting every received
        message.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        raise NotImplementedError("Need to implement and call this on a subclass")

    def _commit(self) -> None:
        offsets = list(self.__offsets.values())
        logger.info(f"Committing offsets: {offsets}")
        self._consumer.commit(offsets=offsets)

    def on_shutdown(self):
        """
        Default behaviour is to ignore what :meth:`pyconnect.pyconnectsink.PyConnectSink.needs_flush` is returning
        and execute one final flush when the connector shuts down.
        This is supposed to make sure that work is committed in case a graceful shutdown occurs before the next flush.
        """
        if self._status == Status.STOPPED:
            self._on_flush()
            # maybe on_flush() returned RUNNING, return STOPPED in that case since we're definitely not running anymore
            if self._status == Status.RUNNING:
                self._status = Status.STOPPED

    def close(self):
        try:
            self._consumer.close()
        except RuntimeError:
            # No problem, consumer already closed
            pass
