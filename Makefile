VERSION := 0.0.2
GROUP := None
SHELL = /bin/bash

install-hooks:
	pre-commit install
	
install-virtualenv:
	poetry install

boot-cluster:
	docker-compose up -d
	scripts/wait-for-it.sh localhost:9092 -t 60 -- echo "Booted up"

run-full-tests: boot-cluster
	poetry run pytest --integration --doctest-modules

run-tests:
	poetry run pytest --doctest-modules

consume-%: boot-cluster
	kafkacat -b broker:9092 -t $*

list-topics: boot-cluster
	kafkacat -b broker:9092 -L

check-offsets: boot-cluster
	test/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group $(GROUP) --offsets --verbose
	test/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group $(GROUP) --state --verbose
	test/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker:9092 --describe --group $(GROUP) --members --verbose

publish-test:
	rm -rf dist
	python setup.py sdist bdist_wheel
	twine upload dist/* --repository-url https://test.pypi.org/legacy/

publish: publish-test
	poetry publish
