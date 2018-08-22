from confluent_kafka import KafkaException
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import List, Dict, Callable, Any
import logging
logger = logging.getLogger(__name__)


class PyConnectException(Exception):
    pass


class Status(Enum):
    NOT_YET_RUNNING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


class BaseConnector(metaclass=ABCMeta):

    def __init__(self):
        self._status = Status.NOT_YET_RUNNING

    def run(self):
        self._before_run_loop()
        self._run_loop()
        self._after_run_loop()
        self._status

    def _run_loop(self):
        while self.is_running:
            self._run_once()

    def _before_run_loop(self):
        if not self.status == Status.NOT_YET_RUNNING:
            raise RuntimeError('Can not re-start a failed/stopped connector, '
                               'need to re-create a Connect instance')

        self._status = Status.RUNNING

        self._on_startup()

    def _handle_kafka_exception(self, e: KafkaException) -> None:
        logger.exception('Kafka internal exception!')
        self._status = Status.CRASHED
        self._status_info = e

    def _handle_general_exception(self, e: Exception):
        logger.exception('Connector crashed!')
        self._status = Status.CRASHED
        self._status_info = e

    def _safe_call_and_set_status(self, callback: Callable,
                                  *args: List[Any],
                                  **kwargs: Dict[Any, Any]) -> None:
        try:
            new_status = callback(*args, **kwargs)
        except Exception as e:
            self._handle_general_exception(e)
            return

        if new_status is None:
            return
        elif isinstance(new_status, Status):
            self._status = new_status
        else:
            raise RuntimeError(f'Callback {callback} needs to return Status '
                               f'but returned {type(new_status)}')

    @abstractmethod
    def _run_once(self) -> None:
        raise NotImplementedError()

    def _after_run_loop(self):
        try:
            self._on_shutdown()
        finally:
            self.close()

    # optional callbacks

    def on_crash(self):
        pass

    def on_shutdown(self):
        pass

    def on_startup(self):
        pass

    # callback wrappers

    def _on_crash(self):
        self._safe_call_and_set_status(self.on_crash)

    def _on_shutdown(self):
        # connector cannot recover during on_shutdown!
        # so no safe call needed
        self.on_shutdown()

    def _on_startup(self):
        self._safe_call_and_set_status(self.on_startup)
