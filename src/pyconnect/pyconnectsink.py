import logging
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
    """
    STANDARD = 0
    NO_MESSAGE = 1
    ERROR = 2


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
            return MessageType.NO_MESSAGE
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

        self.last_message: Message = None
        self._offsets: Dict[Tuple[str, int], TopicPartition] = {}
        self.eof_reached: Dict[Tuple[str, int], bool] = {}

        self._consumer: AvroConsumer = self._make_consumer()

    def _make_consumer(self) -> AvroConsumer:
        config = {
            "bootstrap.servers": ','.join(self.config['bootstrap_servers']),
            "group.id": self.config['group_id'],
            "schema.registry.url": self.config['schema_registry'],

            # We need to commit offsets manually once we"re sure it got saved
            # to the sink
            "enable.auto.commit": False,

            # We need this to start at the last committed offset instead of the
            # latest when subscribing for the first time
            "default.topic.config": {
                "auto.offset.reset": "earliest"
            },
            **self.config['kafka_opts']
        }
        consumer = AvroConsumer(config)
        logging.info(f'AvroConsumer created with config: {config}')
        # noinspection PyArgumentList
        consumer.subscribe(self.config['topics'], on_assign=self._on_assign, on_revoke=self._on_revoke)

        return consumer

    def _on_assign(self, _, partitions: List[TopicPartition]) -> None:
        """
        Handler for topic assignment. When the consumer is assigned to a new topic partition, either during initial
        subscription or later rebalance, then this function is called and will set the EOF reached flag for all
        assigned partitions to `False`.
        This callback is registered automatically on topic subscription.
        """
        logger.info(f'Assigned to partitions: {partitions}')
        for partition in partitions:
            self.eof_reached[(partition.topic, partition.partition)] = False

    def _on_revoke(self, _, partitions: List[TopicPartition]):
        """
        Handler for topic revision. When the consumer is revoked from topic partitions, during rebalance, then this
        function is called and will delete the EOF reached flag for all revoked partitions.
        This callback is registered automatically on topic subscription.
        """
        logger.info(f'Revoked from partitions: {partitions}')
        for partition in partitions:
            del self.eof_reached[(partition.topic, partition.partition)]

    def _run_once(self) -> None:
        try:
            self.last_message = None
            self._status_info = None
            msg = self._consumer.poll(self.config['poll_timeout'])
            self.last_message = msg
            self._call_right_handler_for_message(msg)
            self._flush_if_needed()
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
            logger.debug(f'Message received: {message_repr(msg)}')
        else:
            logger.debug('Message received: None')

        msg_type = determine_message_type(msg)
        if msg_type == MessageType.STANDARD:
            self._on_message_received(msg)
        elif msg_type == MessageType.NO_MESSAGE:
            self._on_no_message_received()
        elif msg_type == MessageType.ERROR:
            self._on_error_received(msg)

    def _on_message_received(self, msg: Message):
        self.eof_reached[(msg.topic(), msg.partition())] = False
        self._update_offset_from_message(msg)
        self._safe_call_and_set_status(self.on_message_received, msg)

    def _update_offset_from_message(self, msg: Message):
        """
        Takes a message and updates the cached offset information with it so offsets are up to date when
        we commit them.
        """
        topic_partition = msg_to_topic_partition(msg)
        topic_partition.offset += 1
        key = (topic_partition.topic, topic_partition.partition)
        self._offsets[key] = topic_partition

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
        if self.last_message is not None:
            # last_message at this point only present if error code was _PARTITION_EOF
            assert self.last_message.error().code() == KafkaError._PARTITION_EOF, 'Message is not EOF!'
            key = (self.last_message.topic(), self.last_message.partition())
            self.eof_reached[key] = True
        self._safe_call_and_set_status(self.on_no_message_received)

    def on_no_message_received(self):
        """
        This callback is called whenever the sink's consumer has not received a message or hit EOF on a partition.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def _on_error_received(self, msg: Message):
        self._safe_call_and_set_status(self.on_error_received, msg)

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
        Called regularly at the end of each run loop cycle in order to determine if
        :meth:`pyconnect.pyconnectsink.PyConnectSink.on_flush` needs to be called.

        Default behaviour is to return True all the time so every message is flushed and its offset committed.

        :return: Boolean indicating whether it's time to flush or not.
        """
        # TODO use config parameter 'flush_interval'
        return True

    def _on_flush(self):
        self._safe_call_and_set_status(self.on_flush)
        if self._status != Status.CRASHED:
            self._safe_call_and_set_status(self._commit)

    @abstractmethod
    def on_flush(self) -> Optional[Status]:
        """
        This callback is called whenever the sink is supposed to flush all messages it has consumed so far.
        There are two situations in which this is the case:

            1. At the end of a run loop cycle, when :meth:`pyconnect.pyconnectsink.PyConnectSink.need_flush` returns
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
        offsets = list(self._offsets.values())
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
