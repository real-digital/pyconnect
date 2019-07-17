from abc import ABCMeta, abstractmethod
from time import sleep
from typing import Any, Optional, Tuple

from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.cimpl import KafkaError, TopicPartition

from .avroparser import to_key_schema, to_value_schema
from .config import SourceConfig
from .core import BaseConnector, PyConnectException, Status


class PyConnectSource(BaseConnector, metaclass=ABCMeta):
    """
    This class offers base functionality for all source connectors. All source connectors have to inherit from this
    class and implement its abstract methods.
    There are also a few optional callbacks that can be overridden if a source implementation needs them.
    For an exemplary implementation of this class have a look at :class:`test.utils.PyConnectTestSource`.
    """

    def __init__(self, config: SourceConfig) -> None:
        super().__init__()
        self.config = config
        self._producer = self._make_producer()
        self._offset_consumer = self._make_offset_consumer()
        self._key_schema: Optional[str] = None
        self._value_schema: Optional[str] = None
        self._offset_schema: Optional[str] = None

    def _make_producer(self) -> AvroProducer:
        """
        Creates the underlying instance of :class:`confluent_kafka.avro.AvroProducer` which is used to publish
        messages and producer offsets.
        """
        config = {
            "bootstrap.servers": ",".join(self.config["bootstrap_servers"]),
            "schema.registry.url": self.config["schema_registry"],
        }
        return AvroProducer(config)

    def _make_offset_consumer(self) -> AvroConsumer:
        """
        Creates the underlying instance of :class:`confluent_kafka.avro.AvroConsumer` which is used to fetch the last
        committed producer offsets.
        """
        config = {
            "bootstrap.servers": ",".join(self.config["bootstrap_servers"]),
            "schema.registry.url": self.config["schema_registry"],
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            "group.id": f'{self.config["offset_topic"]}_fetcher',
            "default.topic.config": {"auto.offset.reset": "latest"},
        }
        offset_consumer = AvroConsumer(config)

        return offset_consumer

    def _before_run_loop(self) -> None:
        super()._before_run_loop()
        idx = self._get_committed_offset()
        if idx is not None:
            self._seek(idx)

    def _get_committed_offset(self) -> Any:
        """
        Fetches the last committed offsets using :attr:`pyconnect.pyconnectsource.PyConnectSource._consumer`.
        """
        self._assign_consumer_to_last_offset()

        offset_msg = self._offset_consumer.poll(timeout=60)
        if offset_msg is None:
            raise PyConnectException("Offset could not be fetched")
        if offset_msg.error() is None:
            return offset_msg.value()
        if offset_msg.error().code() != KafkaError._PARTITION_EOF:
            raise PyConnectException(f"Kafka library returned error: {offset_msg.err().name()}")
        return None

    def _assign_consumer_to_last_offset(self):
        partition = TopicPartition(self.config["offset_topic"], 0)
        _, high_offset = self._offset_consumer.get_watermark_offsets(partition)
        partition.offset = max(0, high_offset - 1)
        self._offset_consumer.assign([partition])

    def _seek(self, idx: Any) -> None:
        self._safe_call_and_set_status(self.seek, idx)

    @abstractmethod
    def seek(self, index: Any) -> Optional[Status]:
        """
        Uses a producer offset to seek to a certain position within the underlying source.
        When this method was called, then the next message read should be the one at `index`.

        :param index: The offset to seek to.
        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        raise NotImplementedError()

    def _run_once(self) -> None:
        try:
            key, value = self.read()
            self._produce(key, value)
            # TODO commit if necessary
        except StopIteration:
            self._on_eof()
        except Exception as e:
            self._handle_exception(e)
        if self._status == Status.CRASHED:
            self._on_crash_during_run()

    @abstractmethod
    def read(self) -> Tuple[Any, Any]:
        """
        Read the current message from the source. Subsequent calls to this method should return subsequent messages.
        Which means that calling it should increment the producer offset.

        :return: A (key, value) tuple representing the record that was read.
        :raises: :exc:`StopIteration` when the end of input source is reached.
        """
        raise NotImplementedError()

    def _produce(self, key: Any, value: Any) -> None:
        """
        Publishes the message given by `key` and `value`.

        :param key: Key for the message that shall be published.
        :param value: Value for the message that shall be published.
        """
        self._create_schemas_if_necessary(key, value)

        self._producer.produce(
            key=key,
            value=value,
            key_schema=self._key_schema,
            value_schema=self._value_schema,
            topic=self.config["topic"],
        )

    def _create_schemas_if_necessary(self, key, value) -> None:
        """
        If no schemas have yet been created, this method will use the `key` and `value` instances to infer one.
        :param key: Key record to infer schema from.
        :param value: Value record to infer schema from.
        """
        if self._key_schema is None:
            self._key_schema = to_key_schema(key)
        if self._value_schema is None:
            self._value_schema = to_value_schema(value)

    def _on_eof(self) -> None:
        self._safe_call_and_set_status(self.on_eof)

    def on_eof(self) -> None:
        """
        This callback is called whenever the end of the input source is reached.
        Default behaviour is to wait 100 ms and try again.
        """
        sleep(0.1)

    def close(self) -> None:
        # TODO this is important! Don't let subclasses overwrite it.
        try:
            self._commit()
            self._offset_consumer.close()
        except RuntimeError:
            pass  # no problem, already closed

    def _commit(self) -> None:
        """
        Retrieves the current offset by calling :meth:`pyconnect.pyconnectsource.PyConnectSource.get_index` and
        publishes it to the offset topic that is defined in this sources :class:`pyconnect.config.SourceConfig`
        instance.
        """
        idx = self.get_index()
        if self._offset_schema is None:
            self._offset_schema = to_value_schema(idx)

        self._producer.produce(
            topic=self.config["offset_topic"], key=None, value=idx, value_schema=self._offset_schema
        )
        self._producer.flush()

    @abstractmethod
    def get_index(self) -> Any:
        """
        Return the offset where the current message is at. I.e. the one which will be next read by
        :meth:`pyconnect.pyconnectsource.PyConnectSource.read`.
        The type of the offset is open and can be whatever the implementing subclass wants it to be. It has to make
        sure, however, that it is compatible with :meth:`pyconnect.pyconnectsource.PyConnectSource.seek`.

        :return: the offset where the current message is at
        """
        raise NotImplementedError()
