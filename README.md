# genetics_spark_coloc
Computing colocalisation analysis using PySpark from credible sets

### Start Dataproc cluster

```bash
# Set parameters.
export CLUSTER_NAME=do-genetics-coloc
export CLUSTER_REGION=europe-west1

# Cluster initialization with pip
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --image-version=2.0 \
    --region=${CLUSTER_REGION} \
    --metadata 'PIP_PACKAGES=omegaconf hydra-core' \
    --initialization-actions gs://goog-dataproc-initialization-actions-europe-west1/python/pip-install.sh                                                  \
    --master-machine-type=n1-highmem-32 \
    --enable-component-gateway \
    --single-node \
    --max-idle=15m
```

### Submit job

```bash
gcloud dataproc jobs submit pyspark run_coloc.py \
    --cluster=${CLUSTER_NAME} \
    --files=config.yaml,coloc.py,colocMetadata.py,overlaps.py \
    --region=${CLUSTER_REGION}
```