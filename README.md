# PyConnect

[![pypi Version](https://img.shields.io/pypi/v/pyconnect.svg)](https://pypi.org/project/pyconnect/) [![Python Versions](https://img.shields.io/pypi/pyversions/pyconnect.svg)](https://pypi.org/project/pyconnect/) [![Build Status](https://travis-ci.org/real-digital/pyconnect.svg?branch=master)](https://travis-ci.org/real-digital/pyconnect) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python implementation of "Kafka Connect"-like functionality that uses plain AvroConsumer/-Producers to make it easier to develop Kafka connect applications.

## TODOs
* Flush consumer messages based on a timeout optionally
* Provide runtime and examples that show how to run it in a dockerized environment
* Add travis build process and mypy checking

## Known issues
* Kafkacat and the Producer will try to reach the test cluster via "localhost" but will get the 
response that they should connect to "broker", which isn't resolvable outside of the docker-compose environment.
That's why we change `/etc/hosts` during development setup. You can undo the changes with `make uninstall-hosts`