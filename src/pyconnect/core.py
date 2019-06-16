"""
This module contains central dependencies for all other modules such as base exceptions or base classes
"""

import logging
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Callable, Optional

from confluent_kafka import KafkaException
from confluent_kafka.cimpl import Message

logger = logging.getLogger(__name__)


def message_repr(msg: Message) -> str:
    """
    Returns out the representation of a :class:`confluent_kafka.Message`

    :param msg: The message which shall be turned into a string.
    :return: String representation of the message.
    """
    return (
        f"Message(key={msg.key()!r}, value={msg.value()!r}, topic={msg.topic()!r}, "
        f"partition={msg.partition()!r}, offset={msg.offset()!r}, error={msg.error()!r})"
    )


class PyConnectException(Exception):
    """
    Base Class for all exceptions raised by the PyConnect framework.
    """

    pass


class NoCrashInfo(PyConnectException):
    """
    Exception that says that a callback returned `Status.CRASHED` without supplying any exception for status_info.
    """

    pass


class Status(Enum):
    """
    The status a connector may be in.

    +================+=====================================================================+
    | Status         | Description                                                         |
    +================+=====================================================================+
    | NOT_YET_RUNNING| Connector was created but has not run yet. Current implementation   |
    |                | does not allow restarting a finished connector. You will have       |
    |                | to create a new one.                                                |
    +----------------+---------------------------------------------------------------------+
    | RUNNING        | Connector is running and has not encountered any problems yet.      |
    +----------------+---------------------------------------------------------------------+
    | STOPPED        | Connector has finished without any problems.                        |
    +----------------+---------------------------------------------------------------------+
    | CRASHED        | Connector has crashed, more information (e.g. the raised exception) |
    |                | can be found within the connectors                                  |
    |                | :attr:`pyconnect.core.BaseConnector.status_info` variable.          |
    +----------------+---------------------------------------------------------------------+
    """

    NOT_YET_RUNNING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


class BaseConnector(metaclass=ABCMeta):
    """
    This class offers basic functionality for both source and sink connectors such as general
    error and status handling or starting the run loop.
    """

    def __init__(self) -> None:
        self._status = Status.NOT_YET_RUNNING
        self._status_info: Optional[Exception] = None

    @property
    def is_running(self) -> bool:
        """
        :return: `True` if the status is RUNNING, `False` otherwise.
        """
        return self._status == Status.RUNNING

    @property
    def status_info(self) -> Any:
        """
        :return: Additional status information for example in case the connector crashed.
        """
        return self._status_info

    @property
    def status(self) -> Status:
        """
        :return: This connectors status.
        """
        return self._status

    def run(self) -> None:
        """
        Prepare, execute and finalize run loop.
        """
        self._before_run_loop()
        self._run_loop()
        self._after_run_loop()

    def _before_run_loop(self) -> None:
        """
        Performs actions necessary for starting the run loop, will also call
        :meth:`pyconnect.core.BaseConnector.on_startup` so implementing classes can do so as well.
        """
        if not self._status == Status.NOT_YET_RUNNING:
            raise PyConnectException(
                "Can not re-start a failed/stopped connector, " "need to re-create a Connect instance"
            )

        self._status = Status.RUNNING

        self._on_startup()

    def _run_loop(self) -> None:
        """
        Calls :meth:`pyconnect.core.BaseConnector._run_once()` until :meth:`pyconnect.core.BaseConnector.is_running`
        returns `False`
        """
        while self.is_running:
            self._run_once()

    @abstractmethod
    def _run_once(self) -> None:
        """
        Contains logic which the connector needs to perform for each message.
        This would be for example reading from source, converting, writing to destination and/or error handling.
        """
        raise NotImplementedError()

    def _after_run_loop(self) -> None:
        """
        Performs actions necessary for finalizing the run loop. Such actions might be to reraise any exceptions caught
        during :meth:`pyconnect.core.Baseconnector._run_once()` or write logging information.
        Will also call :meth:`pyconnect.core.BaseConnector.on_shutdown` so implementing classes can handle shutdown
        as well.
        """
        # TODO assert we're in a legal status here (i.e. CRASHED or STOPPED)
        try:
            self._on_shutdown()
            if self._status == Status.CRASHED and isinstance(self._status_info, Exception):
                raise self._status_info
        finally:
            self.close()

    def _safe_call_and_set_status(self, callback: Callable[..., Optional[Status]], *args, **kwargs) -> None:
        """
        Safely calls a callback and handles any exceptions it raises. Will also update this connector's status *if and
        only if* the callback returns one or fails completely.

        :param callback: Callback that shall be safely called.
        :param args: Arguments to pass on to callback.
        :param kwargs: Keyword arguments to pass on to callback.
        """
        try:
            self._unsafe_call_and_set_status(callback, *args, **kwargs)
        except Exception as e:
            self._handle_exception(e)

    def _unsafe_call_and_set_status(self, callback: Callable[..., Optional[Status]], *args, **kwargs) -> None:
        """
        Calls a callback and updates this connector's status *if and only if* the callback returns one.
        This function does not capture Exceptions. To the contrary: it even raises an exception if the `callback`
        returned :enum:`Status.CRASHED`.

        :param callback: Callback that shall be called.
        :param args: Arguments to pass on to callback.
        :param kwargs: Keyword arguments to pass on to callback.
        """
        new_status = callback(*args, **kwargs)
        if new_status is None:
            return
        elif isinstance(new_status, Status):
            self._status = new_status
            if new_status == Status.CRASHED:
                if self._status_info is None:
                    self._status_info = NoCrashInfo(f"Callback {callback} returned Status CRASHED")
                raise self._status_info
        else:
            raise RuntimeError(f"Callback {callback} needs to return Status or None but returned {type(new_status)}")

    def _handle_exception(self, e: Exception) -> None:
        """
        Handles exceptions raised during execution of the connector.
        Forwards handling of :class:`confluent_kafka.KafkaException` to
        :meth:`pyconnect.core.BaseConnector._handle_kafka_exception`.

        :param e: The exception that was raised.
        """
        if isinstance(e, KafkaException):
            self._handle_kafka_exception(e)
        else:
            logger.exception("Connector crashed!")
            self._status = Status.CRASHED
            self._status_info = e

    def _handle_kafka_exception(self, e: KafkaException) -> None:
        """
        Handles exceptions raised by the kafka client library.

        :param e: The exception that was raised.
        """
        logger.exception("Kafka internal exception!")
        self._status = Status.CRASHED
        self._status_info = e

    @abstractmethod
    def close(self) -> None:
        """
        This method is used for any cleanup operations that have to be done when the connector shuts down.
        It will be called after the run loop, **no matter what**. So it is the perfect place to close sockets or files.
        Calling this method subsequently should *not* raise an exception.
        """
        raise NotImplementedError()

    # optional callbacks

    def on_crash_during_run(self) -> Optional[Status]:
        """
        This method is called whenever the connector has the status :obj:`pyconnect.core.Status.CRASHED` at the end of
        a run loop. Implementing connectors may choose to recover from whatever caused the crash.
        Looking at :attr:`pyconnect.core.BaseConnector.status_info` might help with identifying the underlying issue.

        If the method chooses to recover from the exception, it must return :obj:`pyconnect.core.Status.RUNNING` to
        indicate that the connector is still running.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def on_startup(self) -> Optional[Status]:
        """
        This method is called right before the run loop starts. Implementing connectors may use it to perform any
        preparations necessary before entering the loop.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.
        """
        pass

    def on_shutdown(self) -> Optional[Status]:
        """
        This method is called right after the run loop has stopped.
        It can be used for example to suppress exception propagation in case the connector has crashed during run loop.
        But it can also be used to perform final flush or commit operations. Don't forget, however, to make sure to
        check the connector's status before taking either action.

        :return: A status which will overwrite the current one or `None` if status shall stay untouched.

        .. warning:: This callback **must not** return :obj:`pyconnect.core.Status.RUNNING` or
                     :obj:`pyconnect.core.Status.NOT_YET_RUNNING`!
        """
        pass

    # callback wrappers

    def _on_startup(self) -> None:
        self._safe_call_and_set_status(self.on_startup)

    def _on_crash_during_run(self) -> None:
        self._safe_call_and_set_status(self.on_crash_during_run)

    def _on_shutdown(self) -> None:
        self._safe_call_and_set_status(self.on_shutdown)
        assert self._status in (Status.STOPPED, Status.CRASHED), f"Illegal State: {self._status.name} after shutdown!"
