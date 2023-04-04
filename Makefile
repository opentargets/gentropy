PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean: ## CleanUp Prior to Build
	@rm -Rf ./dist

setup-dev: SHELL:=/bin/bash
setup-dev: ## Setup dev environment
	@. utils/install_dependencies.sh

build: clean ## Build Python Package with Dependencies
	@gcloud config set project ${PROJECT_ID}
	@echo "Packaging Code and Dependencies for ${APP_NAME}-${VERSION_NO}"
	@rm -rf ./dist
	@poetry build
	@tar -czf dist/config.tar.gz config/
	@echo "Uploading to Dataproc"
	@gsutil cp src/otg/cli.py ${BUCKET_NAME}
	@gsutil cp ./dist/${APP_NAME}-${VERSION_NO}-py3-none-any.whl ${BUCKET_NAME}
	@gsutil cp ./dist/config.tar.gz ${BUCKET_NAME}
	@gsutil cp ./utils/initialise_cluster.sh ${BUCKET_NAME}
