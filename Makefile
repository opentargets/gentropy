PROJECT_ID ?= open-targets-genetics-dev
REGION ?= europe-west1
CLUSTER_NAME ?= ds-genetics-python-etl
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
	@poetry build
	@cp ./src/*.py ./dist
	@poetry run python ./utils/configure.py --cfg job > ./dist/config.yaml
	@echo "Uploading to Dataproc"
	@gsutil cp ./dist/${APP_NAME}-${VERSION_NO}-py3-none-any.whl gs://genetics_etl_python_playground/initialisation/
	@gsutil cp ./utils/initialise_cluster.sh gs://genetics_etl_python_playground/initialisation/

prepare_pics:  ## Create cluster for variant annotation
	gcloud dataproc clusters create ${CLUSTER_NAME} \
        --image-version=2.0 \
        --project=${PROJECT_ID} \
        --region=${REGION} \
		--master-machine-type=n1-highmem-96 \
        --enable-component-gateway \
        --metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl" \
        --initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
        --single-node \
        --max-idle=10m

prepare_variant_annotation:  ## Create cluster for variant annotation
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-standard-96 \
		--enable-component-gateway \
		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl" \
		--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
		--single-node \
		--max-idle=10m

prepare_intervals: ## Create cluster for intervals data generation
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-highmem-32 \
		--enable-component-gateway \
		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl" \
		--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
		--single-node \
		--max-idle=10m

prepare_coloc: ## Create cluster for coloc
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-highmem-64 \
		--num-master-local-ssds=1 \
		--master-local-ssd-interface=NVME \
		--enable-component-gateway \
		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl" \
		--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
		--single-node \
		--max-idle=10m

prepare_gwas: ## Create cluster for gwas data generation
	gcloud dataproc clusters create ${CLUSTER_NAME} \
		--image-version=2.0 \
		--project=${PROJECT_ID} \
		--region=${REGION} \
		--master-machine-type=n1-highmem-32 \
		--metadata="PACKAGE=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl" \
		--initialization-actions=gs://genetics_etl_python_playground/initialisation/initialise_cluster.sh \
		--enable-component-gateway \
		--single-node \
		--max-idle=10m

run_coloc: ## Generate coloc results
	gcloud dataproc jobs submit pyspark ./dist/run_coloc.py \
    --cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl \
    --project=${PROJECT_ID} \
    --region=${REGION}

run_intervals: ## Generate intervals dataset
	gcloud dataproc jobs submit pyspark ./dist/run_intervals.py \
	--cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl \
    --project=${PROJECT_ID} \
    --region=${REGION}

run_variant_annotation: ## Generate variant annotation dataset
	gcloud dataproc jobs submit pyspark ./dist/run_variant_annotation.py \
	--cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl \
    --project=${PROJECT_ID} \
    --region=${REGION}

run_gwas: ## Ingest gwas dataset on a dataproc cluster
	gcloud dataproc jobs submit pyspark ./dist/run_gwas_ingest.py \
	--cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
    --py-files=gs://genetics_etl_python_playground/initialisation/${APP_NAME}-${VERSION_NO}-py3-none-any.whl \
    --project=${PROJECT_ID} \
    --region=${REGION}

run_pics: ## Generate variant annotation dataset
	gcloud dataproc jobs submit pyspark ./dist/pics_experiment.py \
    --cluster=${CLUSTER_NAME} \
    --files=./dist/config.yaml \
	--properties='spark.jars=/opt/conda/miniconda3/lib/python3.8/site-packages/hail/backend/hail-all-spark.jar,spark.driver.extraClassPath=/opt/conda/miniconda3/lib/python3.8/site-packages/hail/backend/hail-all-spark.jar,spark.executor.extraClassPath=./hail-all-spark.jar,spark.serializer=org.apache.spark.serializer.KryoSerializer,spark.kryo.registrator=is.hail.kryo.HailKryoRegistrator' \
	--project=${PROJECT_ID} \
    --region=${REGION}
