PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
ZONE=europe-west1-d
BUCKET_NAME=gs://genetics_etl_python_playground/initialisation/
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
TEMPLATE_ID=${USER}-otgenetics-template
STEP_ID=my_gene_index
# CLUSTER_NAME ?= ${USER}-genetics-etl
# PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)")


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

template:
	@gcloud dataproc workflow-templates create ${TEMPLATE_ID} --region ${REGION}

add_cluster:
	@gcloud dataproc workflow-templates set-managed-cluster \
	${TEMPLATE_ID} \
	--project=${PROJECT_ID} \
	--region ${REGION} \
	--zone ${ZONE} \
	--image-version=2.0 \
	--master-machine-type=n1-highmem-32 \
	--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl,CONFIGTAR=gs://genetics_etl_python_playground/initialisation/config.tar.gz" \
	--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
	--enable-component-gateway \
	--single-node

test:
	@gcloud dataproc clusters create do-test \
	--region ${REGION} \
	--zone ${ZONE} \
	--image-version=2.0 \
	--master-machine-type=n1-highmem-32 \
	--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl,CONFIGTAR=gs://genetics_etl_python_playground/initialisation/config.tar.gz" \
	--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
	--enable-component-gateway \
	--single-node


add_gene_index_step:
	@gcloud dataproc workflow-templates add-job pyspark gs://genetics_etl_python_playground/initialisation/cli.py \
	--workflow-template=${TEMPLATE_ID} \
	--step-id=${STEP_ID} \
	--region=${REGION} \
	-- step=${STEP_ID} --config-dir=/config -cn=my_config

instantiate:
	time gcloud dataproc workflow-templates instantiate \
	  ${TEMPLATE_ID} --region ${REGION} #--async
