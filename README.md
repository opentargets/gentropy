[![status: experimental](https://github.com/GIScience/badges/raw/master/status/experimental.svg)](https://github.com/GIScience/badges#experimental)

# genetics_spark_coloc
Computing colocalisation analysis using PySpark from credible sets

### Start Dataproc cluster

```bash
# Set parameters.
export PROJECT=open-targets-genetics-dev
export CLUSTER_NAME=do-genetics-coloc
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
```

### Submit job

```bash
gcloud dataproc jobs submit pyspark run_coloc.py \
    --cluster=${CLUSTER_NAME} \
    --files=config.yaml\
    --py-files=coloc.py,colocMetadata.py,overlaps.py \
    --project=${PROJECT} \
    --region=${CLUSTER_REGION}
```
