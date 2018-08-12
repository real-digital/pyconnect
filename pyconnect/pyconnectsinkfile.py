from .pyconnectsink import PyConnectSink, Status, Callback
from typing import TYPE_CHECKING, Dict, Any, Callable, Optional
import json

if TYPE_CHECKING:
    from confluent_kafka import Message

def noop(*args, **kwargs):
    pass



class PyConnectSinkFile(PyConnectSink):

    def __init__(self, **config: Dict[str, Any]) -> None:
        super().__init__(**config)
        self.filename: str = config["filename"]
        self.on_message_handled: Callable = config.get("on_message_handled", noop)
        self.on_empty_poll: Callable = config.get("on_empty_poll", noop)

    def handle_message(self, msg: "Message"):
        with open(self.filename, "a") as outfile:
            outfile.write(json.dumps(msg.value()) + "\n")
