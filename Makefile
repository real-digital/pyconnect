

install-dev-env:
	sudo apt-get install docker docker-compose -y
	pip install -r requirements.txt

reset-cluster:
	sudo docker-compose -f test/testenv-docker-compose.yml rm -f

boot-cluster: reset-cluster
	sudo docker-compose -f test/testenv-docker-compose.yml up --force-recreate

run-tests: boot-cluster
	pytest