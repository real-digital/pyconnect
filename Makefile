

install-dev-env:
	sudo apt-get install docker docker-compose kafkacat -y
	pip install -r requirements.txt

reset-cluster:
	sudo docker-compose -f test/testenv-docker-compose.yml rm -f

boot-cluster: reset-cluster
	sudo docker-compose -f test/testenv-docker-compose.yml up --force-recreate

run-tests: boot-cluster
	pytest

consume-%:
	kafkacat -b localhost:9092 -t $^

list-topics:
	kafkacat -b localhost:9092 -L

GROUP := None

check-offsets:
	../confluent/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $(GROUP) --offsets --verbose
	../confluent/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $(GROUP) --state --verbose
	../confluent/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group $(GROUP) --members --verbose