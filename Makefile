PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
APP_NAME ?= $$(cat pyproject.toml | grep -m 1 "name" | cut -d" " -f3 | sed  's/"//g')
REF ?= $$(git rev-parse --abbrev-ref HEAD)
PACKAGE_VERSION ?= $$(poetry version --short)
CLEAN_PACKAGE_VERSION := $(shell echo "$(PACKAGE_VERSION)" | tr -cd '[:alnum:]')
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/${APP_NAME}/${REF}

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean: ## Clean up prior to building
	@rm -Rf ./dist

setup-dev: SHELL:=/bin/bash
setup-dev: ## Setup development environment
	@. utils/install_dependencies.sh

check: ## Lint and format code
	@echo "Linting API..."
	@poetry run ruff check src/gentropy .
	@echo "Linting docstrings..."
	@poetry run pydoclint --config=pyproject.toml src
	@poetry run pydoclint --config=pyproject.toml --skip-checking-short-docstrings=true tests

test: ## Run tests
	@echo "Running Tests..."
	@poetry run pytest

build-documentation: ## Create local server with documentation
	@echo "Building Documentation..."
	@poetry run mkdocs serve

create-dev-cluster: build ## Spin up a simple dataproc cluster with all dependencies for development purposes
	@echo "Creating Dataproc Dev Cluster"
	@gcloud config set project ${PROJECT_ID}
	@gcloud dataproc clusters create "ot-genetics-dev-${CLEAN_PACKAGE_VERSION}-$(USER)" \
		--image-version 2.1 \
		--region ${REGION} \
		--master-machine-type n1-standard-16 \
		--initialization-actions=$(BUCKET_NAME)/install_dependencies_on_cluster.sh \
		--metadata="PACKAGE=$(BUCKET_NAME)/${APP_NAME}-${PACKAGE_VERSION}-py3-none-any.whl" \
		--secondary-worker-type spot \
		--worker-machine-type n1-standard-4 \
		--worker-boot-disk-size 500 \
		--autoscaling-policy="projects/${PROJECT_ID}/regions/${REGION}/autoscalingPolicies/otg-etl" \
		--optional-components=JUPYTER \
		--enable-component-gateway \
		--max-idle=60m

make update-dev-cluster: build ## Reinstalls the package on the dev-cluster
	@echo "Updating Dataproc Dev Cluster"
	@gcloud config set project ${PROJECT_ID}
	gcloud dataproc jobs submit pig --cluster="ot-genetics-dev-${CLEAN_PACKAGE_VERSION}" \
		--region ${REGION} \
		--jars=${BUCKET_NAME}/install_dependencies_on_cluster.sh \
		-e='sh chmod 750 $${PWD}/install_dependencies_on_cluster.sh; sh $${PWD}/install_dependencies_on_cluster.sh'

build: clean ## Build Python package with dependencies
	@gcloud config set project ${PROJECT_ID}
	@echo "Packaging Code and Dependencies for ${APP_NAME}-${PACKAGE_VERSION}"
	@poetry build
	@echo "Uploading to ${BUCKET_NAME}"
	@gsutil cp src/${APP_NAME}/cli.py ${BUCKET_NAME}/
	@gsutil cp ./dist/${APP_NAME}-${PACKAGE_VERSION}-py3-none-any.whl ${BUCKET_NAME}/
	@gsutil cp ./utils/install_dependencies_on_cluster.sh ${BUCKET_NAME}/
