import json
from math import inf
from typing import TYPE_CHECKING, Dict, Any, Callable

from pyconnect.pyconnectsink import PyConnectSink

if TYPE_CHECKING:
    from confluent_kafka import Message


def noop(*args, **kwargs):
    pass


class PyConnectSinkFile(PyConnectSink):

    def __init__(self, **config: Dict[str, Any]) -> None:
        super().__init__(**config)
        self.filename: str = config["filename"]

        self.fail_before_counter = config.get("fail_before_counter", inf)
        self.fail_after_counter = config.get("fail_after_counter", inf)

        # Those two should typically be set in the class definition, but we provide dynamic hooks for tests here
        self.on_message_handled: Callable = config.get("on_message_handled", noop)
        self.on_empty_poll: Callable = config.get("on_empty_poll", noop)

        self.file = open(self.filename, "a")

    def handle_message(self, msg: "Message"):

        self.fail_before_counter -= 1
        if self.fail_before_counter == 0:
            raise ValueError("Fail before counter reached")

        self.file.write(json.dumps(msg.value()) + "\n")

        self.fail_after_counter -= 1
        if self.fail_after_counter == 0:
            raise ValueError("Fail after counter reached")

    def stop(self):
        self.file.close()

    def crash(self):
        self.file.close()
