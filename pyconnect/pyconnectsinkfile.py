from .pyconnectsink import PyConnectSink
from typing import TYPE_CHECKING, Dict, Any
import json

if TYPE_CHECKING:
    from confluent_kafka import Message


class PyConnectSinkFile(PyConnectSink):

    def __init__(self, **config: Dict[str, Any]) -> None:
        super().__init__(**config)
        self.filename: str = config["filename"]

    def handle_message(self, msg: "Message"):
        with open(self.filename, "a") as outfile:
            outfile.write(json.dumps(msg.value()) + "\n")
