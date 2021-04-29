### Version 0.5.1
* use poetry to publish packages
### Version 0.5.0
#### Breaking changes
* update confluent-kafka-python to version 1.6.1
* Use DeserializingConsumer instead of deprecated AvroConsumer and SerializingProducer instead of AvroProducer
* remove RichAvroConsumer
#### Other changes
* replace pipenv with poetry
* use docker-compose for tests
### Version 0.4.5
* Enabled the Sink to retry committing offsets in case of failure. The maximum number of retries is configurable through 
the `sink_commit_retry_count` field.
### Version 0.4.4
* Enable JSON logging using loguru
### Version 0.4.3
* Fix bug fix introduced in Version 0.4.2, only commit offsets if exist.
### Version 0.4.2
* Make consumer offset commits synchronous.
### Version 0.4.1
* Introduce hashing of sensitive values.
