# CHEERS_on_Spark
CHEERS implemented using SPARK

# Set parameters
export PROJECT=open-targets-genetics-dev

export CLUSTER_NAME=CHEERS_on_SPARK

export CLUSTER_REGION=europe-west1

# Cluster initialization with pip
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.0 \
    --project=${PROJECT} \
    --region=${CLUSTER_REGION} \
    --metadata 'PIP_PACKAGES=omegaconf hydra-core' \
    --initialization-actions gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh                                                  \
    --master-machine-type=n1-highmem-64 \
    --num-master-local-ssds=1 \
    --master-local-ssd-interface=NVME \
    --enable-component-gateway \
    --single-node \
    --max-idle=10m

# Submit job
gcloud dataproc jobs submit OTG_CHEERS_enrichment.py \
    --cluster=${CLUSTER_NAME} \
    --files=config.yaml\
    --py-files=Utils.py \
    --project=${PROJECT} \
    --region=${CLUSTER_REGION}

