PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
CLUSTER_NAME ?= do-genetics-coloc
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)")
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
SRC_WITH_DEPS ?= code_bundle

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean: ## CleanUp Prior to Build
	@rm -Rf ./dist
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt

setup-dev: ## Setup dev environment
	@echo "Installing dependencies..."
	@poetry install --remove-untracked
	@echo "Setting up pre-commit..."
	@poetry run pre-commit install
	@poetry run pre-commit autoupdate
	@poetry run pre-commit install --hook-type commit-msg
	@echo "You are ready to code!"

build: clean ## Build Python Package with Dependencies
	@echo "Packaging Code and Dependencies for ${APP_NAME}-${VERSION_NO}"
	@mkdir -p ./dist
	@poetry update
	@poetry export -f requirements.txt --without-hashes -o requirements.txt
	@poetry run pip install . -r requirements.txt -t ${SRC_WITH_DEPS}
	@cd ./${SRC_WITH_DEPS}
	@find . -name "*.pyc" -delete
	@cd ./${SRC_WITH_DEPS} && zip -x "*.git*" -x "*.DS_Store" -x "*.pyc" -x "*/*__pycache__*/" -x ".idea*" -r ../dist/${SRC_WITH_DEPS}.zip .
	@mv ./dist/${SRC_WITH_DEPS}.zip ./dist/${APP_NAME}_${VERSION_NO}.zip
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt
	@cp ./src/*.py ./dist
	@poetry run python ./utils/configure.py --cfg job > ./dist/config.yaml

prepare_coloc: ## Create machine for coloc
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-highmem-64 \
		--num-master-local-ssds=1 \
		--master-local-ssd-interface=NVME \
		--enable-component-gateway \
		--single-node \
		--max-idle=10m

run_coloc: ## Submit coloc job to created machine
	gcloud dataproc jobs submit pyspark ./dist/run_coloc.py \
    --cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=./dist/${APP_NAME}_${VERSION_NO}.zip \
    --project=${PROJECT_ID} \
    --region=${REGION}


prepare_intervals: ## Create cluster for interval data generation
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-highmem-32 \
		--enable-component-gateway \
		--single-node \
		--max-idle=10m

run_intervals: ## Generate intervals dataset on a serverless dataproc cluster
	gcloud dataproc jobs submit pyspark ./dist/run_intervals.py \
	--cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=./dist/${APP_NAME}_${VERSION_NO}.zip \
    --project=${PROJECT_ID} \
    --region=${REGION}
