from abc import ABCMeta, abstractmethod
from confluent_kafka.avro import AvroProducer, AvroConsumer
from typing import Any, Tuple, Optional
from time import sleep

from confluent_kafka.cimpl import TopicPartition, OFFSET_END, KafkaError

from pyconnect.avroparser import to_value_schema, to_key_schema
from pyconnect.config import SourceConfig
from pyconnect.core import BaseConnector, Status, PyConnectException


class PyConnectSource(BaseConnector, metaclass=ABCMeta):

    def __init__(self, config: SourceConfig) -> None:
        super().__init__()
        self.config = config
        self._producer = self._make_producer()
        self._offset_consumer = self._make_offset_consumer()
        self._key_schema: str = None
        self._value_schema: str = None
        self._offset_schema: str = None

    def _make_offset_consumer(self) -> AvroConsumer:
        config = {
            'bootstrap.servers': ','.join(self.config.bootstrap_servers),
            'schema.registry.url': self.config.schema_registry,
            'enable.auto.commit': False,
            'offset.store.method': 'none',
            'group.id': f'{self.config.offset_topic}_fetcher',
            'default.topic.config': {
                'auto.offset.reset': 'latest'
            },
        }
        offset_consumer = AvroConsumer(config)
        offset_consumer.assign([TopicPartition(self.config.offset_topic, 0, OFFSET_END)])

        return offset_consumer

    def _make_producer(self) -> AvroProducer:
        config = {
            'bootstrap.servers': ','.join(self.config.bootstrap_servers),
            'schema.registry.url': self.config.schema_registry
        }
        return AvroProducer(config)

    def _on_eof(self) -> None:
        self._safe_call_and_set_status(self.on_eof)

    def _seek(self, idx: Any) -> None:
        self._safe_call_and_set_status(self.seek, idx)

    def _produce(self, key, value):
        self._create_schemas_if_necessary(key, value)

        self._producer.produce(key=key, value=value,
                               key_schema=self._key_schema,
                               value_schema=self._value_schema,
                               topic=self.config.topic)

    def _create_schemas_if_necessary(self, key, value):
        if self._key_schema is None:
            self._key_schema = to_key_schema(key)
        if self._value_schema is None:
            self._value_schema = to_value_schema(value)

    def _get_committed_offset(self) -> Any:
        partition = self._offset_consumer.assignment()[0]
        _, high_offset = self._offset_consumer.get_watermark_offsets(partition)
        partition.offset = high_offset - 1
        self._offset_consumer.seek(partition)

        offset_msg = self._offset_consumer.poll(timeout=30)
        if offset_msg is None:
            raise PyConnectException('Offset could not be fetched')
        if offset_msg.error() is None:
            return offset_msg.value()
        if offset_msg.error().code() != KafkaError._PARTITION_EOF:
            raise PyConnectException(f'Kafka library returned error: {offset_msg.err().name()}')
        return None

    def _before_run_loop(self) -> None:
        super()._before_run_loop()
        idx = self._get_committed_offset()
        if idx is not None:
            self._safe_call_and_set_status(self.seek, idx)

    def _commit(self) -> None:
        idx = self.get_index()
        if self._offset_schema is None:
            self._offset_schema = to_value_schema(idx)

        self._producer.produce(topic=self.config.offset_topic, key=None, value=idx,
                               value_schema=self._offset_schema)
        self._producer.flush()

    def _run_once(self) -> None:
        try:
            key, value = self.read()
            self._produce(key, value)
        except StopIteration:
            self._on_eof()
        except Exception as e:
            self._handle_general_exception(e)
        if self._status == Status.CRASHED:
            self._on_crash()


    def close(self) -> None:
        try:
            self._commit()
            self._offset_consumer.close()
        except RuntimeError:
            pass  # no problem, already closed

    def on_eof(self) -> None:
        sleep(0.1)

    @abstractmethod
    def get_index(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def read(self) -> Tuple[Any, Any]:
        raise NotImplementedError()

    @abstractmethod
    def seek(self, index: Any) -> Optional[Status]:
        raise NotImplementedError()
