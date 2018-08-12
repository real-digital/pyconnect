# PyConnect

A Python implementation of "Kafka Connect"-like functionality that uses plain AvroConsumer/-Producers to make it easier to develop Kafka connect applications.

## TODOs
* Write SourceConnector, currently only sinks are available
* Flush consumer messages based on a timeout optionally
* Provide runtime and examples that show how to run it in a dockerized environment
* Add travis build process and mypy checking

## Known issues
* Kafkacat and the Producer will try to reach the test cluster via "localhost" but will get the 
response that they should connect to "broker", which isn't resolvable outside of the docker-compose environment.
A simple fix is to just add "broker" as an alias for "localhost" in `/etc/hosts`.