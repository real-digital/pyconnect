import json
from typing import Dict, Any, List, Callable

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import KafkaError

from time import sleep

from enum import Enum

def noop(*args, **kwargs):
    pass

class Status(Enum):
    INITIALIZING = 0
    RUNNING = 1
    STOPPED = 2
    CRASHED = 3


class PyConnectSinkFile:

    def __init__(self, **config: Dict[str, Any]):
        self.connect_name: str = config["connect_name"]
        self.brokers: str = config["brokers"]
        self.topic: str = config["topic"]
        self.schema_registry: str = config["schema_registry"]
        self.flush_after: int = config["flush_after"]
        self.on_message_handled: Callable = config.get("on_message_handled", noop)
        self.on_empty_poll: Callable = config.get("on_empty_poll", noop)
        self.state: Status = Status.INITIALIZING

        self.processed: int = 0
        self.poll_timeout: int = config.get("poll_timeout", 0.5)

        self.filename: str = config["filename"]

        self._consumer: AvroConsumer = None
        self._init_consumer()

    def _init_consumer(self):
        self._consumer = AvroConsumer(self._get_consumer_config())
        self._consumer.subscribe([self.topic])

    def _get_consumer_config(self):
        return {
            "bootstrap.servers": self.brokers,
            "group.id": self.connect_name,
            "schema.registry.url": self.schema_registry,
            'default.topic.config':
                {
                    'auto.offset.reset': 'smallest'
                }
        }

    def _handle_message_internal(self, msg):
        self.handle_message(msg)
        self.processed += 1
        if self.processed % self.flush_after:
            self._flush_back()
        self._run_callback(self.on_message_handled)

    def _run_callback(self, callback, *args, **kwargs):

        new_status = callback(self, *args, **kwargs)
        if new_status is None:
            return
        if new_status not in Status:
            raise ValueError(f"Callback {str(callback)} must either return None or a valid Status")
        self.status = new_status

    def _flush_back(self):
        pass  # TODO Write test for that first

    def handle_message(self, msg):
        with open(self.filename, "a") as outfile:
            outfile.write(json.dumps(msg.value()) + "\n")

    def stop(self):
        self.status = Status.STOPPED

    def run(self):
        self.status = Status.RUNNING
        while self.status == Status.RUNNING:
            msg = self._consumer.poll(self.poll_timeout)
            self._handle_response(msg)
        return None

    def _handle_response(self, msg):
        # Both of those cases are simple "we have no further messages available" messages
        # EOF is returned the first time we hit the EOF, from then on we get None's afer the poll()'s timeout runs out
        if msg is None or (msg.error() and msg.error().code() == KafkaError._PARTITION_EOF):
            self._run_callback(self.on_empty_poll)
        elif msg.error():
                self.status = Status.CRASHED
                print("FATAL::", msg.error())
        else:
            self._handle_message_internal(msg)