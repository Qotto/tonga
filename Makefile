# Testing aioevent (UNIX only)

setup:
	pipenv install --system --dev

dev-repo:
	cp ./recipes/2019_05_31_dev_repo.sh .
	./2019_05_31_dev_repo.sh
	rm ./2019_05_31_dev_repo.sh

up-dev-env:
	docker-compose up -d --build

test:
	tox

down-dev-env:
	docker-compose stop

clean-dev-env:
	docker-compose rm -f
	docker volume rm dev_env_kafka_data_1
	docker volume rm dev_env_kafka_data_2
	docker volume rm dev_env_kafka_data_3
	docker volume rm dev_env_zoo_data
	docker volume rm dev_env_zoo_log_data

clean-repo:
	cp ./recipes/2019_05_31_clean_repo.sh .
	./2019_05_31_clean_repo.sh
	rm ./2019_05_31_clean_repo.sh

.PHONY: all coverage test cov clean tox
