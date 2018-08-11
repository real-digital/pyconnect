

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

check-connect-offsets:
	../confluent/bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --from-beginning --topic _pyconnect_offsets