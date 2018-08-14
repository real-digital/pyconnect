VERSION := 0.0.2
GROUP := None
SHELL = /bin/bash

install-system-packages:
	sudo apt-get install docker docker-compose kafkacat python-virtualenv python3.7 -y

install-virtualenv:
	[[ -d .venv ]] || virtualenv --python=3.7 ./.venv
	./.venv/bin/python -m pip install -r requirements.txt

install-hosts:
	[[ -n "`cat /etc/hosts | grep __start_pyconnect__`" ]] || \
	(cat ./hosts.template | sudo tee -a /etc/hosts)

uninstall-hosts:
	sudo sed -i /__start_pyconnect__/,/__stop_pyconnect__/d /etc/hosts


install-dev-env: install-system-packages install-virtualenv install-hosts

reset-cluster:
	sudo docker-compose -f test/testenv-docker-compose.yml rm -f

boot-cluster: reset-cluster
	@( \
	  (curl -s "http://rest-proxy:8082/topics" >/dev/null) && \
	  (echo "Cluster already running.") \
	) \
	  || \
	( \
	  (echo "Starting Cluster") && \
	  (sudo docker-compose -f test/testenv-docker-compose.yml up --force-recreate -d) && \
	  (until (curl -s "http://rest-proxy:8082/topics" >/dev/null); do sleep 0.1s; done) \
	)

run-tests: boot-cluster
	.venv/bin/python -m pytest

consume-%: boot-cluster
	kafkacat -b broker:9092 -t $*

list-topics: boot-cluster
	kafkacat -b broker:9092 -L

check-offsets: boot-cluster
	../confluent/bin/kafka-consumer-groups --bootstrap-server broker:9092 --describe --group $(GROUP) --offsets --verbose
	../confluent/bin/kafka-consumer-groups --bootstrap-server broker:9092 --describe --group $(GROUP) --state --verbose
	../confluent/bin/kafka-consumer-groups --bootstrap-server broker:9092 --describe --group $(GROUP) --members --verbose

publish-test:
	python setup.py sdist
	twine upload dist/* -r testpypi

publish: publish-test
	twine upload dist/*