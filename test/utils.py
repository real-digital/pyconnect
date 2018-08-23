import pytest
import string
import random


class ConnectTestMixin():

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.forced_status_after_run = None
        self.run_counter = 0
        self.max_runs = 20

    def _run_once(self):
        if self.run_counter >= self.max_runs:
            pytest.fail('Runlimit Reached! Forgot to force stop?')
        self.run_counter += 1

        super()._run_once()

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
