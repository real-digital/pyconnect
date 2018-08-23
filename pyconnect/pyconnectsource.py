from abc import ABCMeta, abstractmethod
from confluent_kafka.avro import AvroProducer
from typing import Any, Tuple
from time import sleep

from pyconnect.config import SourceConfig
from pyconnect.core import BaseConnector


class PyConnectSource(BaseConnector, metaclass=ABCMeta):

    def __init__(self, config: SourceConfig) -> None:
        super().__init__()
        self.config = config
        self._producer = self._make_producer()
        # TODO get last offset and seek

    def _make_producer(self) -> AvroProducer:
        config = {
            'bootstrap.servers': ','.join(self.config.bootstrap_servers),
            'schema.registry.url': self.config.schema_registry
        }
        return AvroProducer(config)

    def _on_eof(self) -> None:
        self._safe_call_and_set_status(self.on_eof)

    def _produce(self, key, value):
        self._producer.produce(key=key, value=value)

    def _run_once(self) -> None:
        try:
            key, value = self.read()
            self._produce(key, value)
        except StopIteration:
            self._on_eof()
        except Exception as e:
            self._handle_general_exception(e)

    def close(self) -> None:
        try:
            self._producer.close()
        except RuntimeError:
            pass  # no problem, already closed

    @abstractmethod
    def read(self) -> Tuple[Any, Any]:
        raise NotImplementedError()

    @abstractmethod
    def seek(self, index: Any):
        raise NotImplementedError()

    # create producer with config (registers for data and offset topics)
    # run
    #     obtain last offset position
    #     call seek with offset
    #     while status is RUNNING
    #         call read_with_timeout (which calls read, and sleeps)
    #         publish message
    #         if commit_condition is met (i.e. need_commit_offset() == True)
    #             call get_current_offset
    #             send offset to offset_topic
