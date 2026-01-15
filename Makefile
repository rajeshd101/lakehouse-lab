include dbt_project/dbt.env

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			helpDir = match(lastLine, /@`([^`]+)`/); \
			if (helpDir) { \
				helpDir = substr(lastLine, RSTART + 2, RLENGTH - 3); \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET} in `%s`\n", helpCommand, helpMessage, helpDir; \
			} else { \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
			} \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)


.PHONY: setup-venv
## Create a local virtual environment with Python 3.12
setup-venv:
	@if [ -d venv ]; then rm -rf venv; fi
	py -3.12 -m venv venv
	@echo "Virtual environment created with Python 3.12."
	@venv\Scripts\python.exe -m pip install --upgrade pip
	@venv\Scripts\python.exe -m pip install -r requirements.txt
	@echo "Dependencies installed."

.PHONY: dbt-run
## Activate the virtual environment and source dbt.env
dbt-run:
	@if [ -d dbt_project/dbt_venv ]; then rm -rf dbt_project/dbt_venv; fi
	py -3.12 -m venv dbt_project/dbt_venv
	@echo "Virtual environment created with Python 3.12."
	@. dbt_project/dbt_venv/bin/activate && \
	pip install --upgrade pip && \
	pip install -r dbt_project/dbt-requirements.txt && \
	source dbt_project/dbt.env && \
	cd dbt_project && \
	exec /bin/bash

.PHONY: services-up
## Start Trino, Hive Metastore, and Spark services via @`docker-compose.yml`
services-up:
	docker compose up -d hive-metastore-db hive-metastore trino spark-master spark-worker

.PHONY: services-down
## Stop Trino, Hive Metastore, and Spark services via @`docker-compose.yml`
services-down:
	docker compose down

.PHONY: astro-start
## Start the Airflow stack via @`astro dev start`
astro-start:
	astro dev start

.PHONY: astro-connect
## Attach Astro containers to the shared Docker network @`lakehouse_lab_lakehouse-net`
astro-connect:
	@docker network connect lakehouse_lab_lakehouse-net lakehouse-rjd-airflow-webserver || true
	@docker network connect lakehouse_lab_lakehouse-net lakehouse-rjd-airflow-scheduler || true

.PHONY: astro-stop
## Stop the Airflow stack via @`astro dev stop`
astro-stop:
	astro dev stop

.PHONY: compose-up
## Start full stack via @`docker-compose.yml`
compose-up:
	docker compose up -d

.PHONY: compose-down
## Stop full stack via @`docker-compose.yml`
compose-down:
	docker compose down

.PHONY: dev-up
## Start full stack via @`docker-compose.yml`
dev-up: compose-up

.PHONY: dev-down
## Stop full stack via @`docker-compose.yml`
dev-down: compose-down
