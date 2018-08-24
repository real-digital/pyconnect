from typing import Any
from confluent_kafka import Message
from confluent_kafka import avro as confluent_avro
import pytest
import string
import random
import json
import os

from pyconnect.avroparser import create_schema_from_record

TEST_DIR = os.path.abspath(os.path.dirname(__file__))
CLI_DIR = os.path.join(TEST_DIR, 'kafka', 'bin')


class ConnectTestMixin():

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.forced_status_after_run = None
        self.run_counter = 0
        self.max_runs = 20

    def _run_loop(self):
        while self.is_running:
            if self.run_counter >= self.max_runs:
                pytest.fail('Runlimit Reached! Forgot to force stop?')
            self.run_counter += 1

            self._run_once()

            if isinstance(self.forced_status_after_run, list):
                if len(self.forced_status_after_run) > 1:
                    new_status = self.forced_status_after_run.pop(0)
                else:
                    new_status = self.forced_status_after_run[0]
            else:
                new_status = self.forced_status_after_run

            if new_status is not None:
                self._status = new_status


def rand_text(textlen):
    return ''.join(random.choices(string.ascii_uppercase, k=textlen))


def to_schema(name: str, record: Any):
    return confluent_avro.loads(json.dumps(
        create_schema_from_record(name, record)))


def message_repr(msg: Message):
    return (
        f'Message(key={msg.key()!r}, value={msg.value()!r}, '
        f'topic={msg.topic()!r}, partition={msg.partition()!r}, '
        f'offset={msg.offset()!r}, error={msg.error()!r})'
    )
