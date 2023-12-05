# Pipeline workflows

This page describes the high level components of the pipeline, which are organised as Airflow DAGs (directed acyclic graphs).

## Note on DAGs and Dataproc clusters

Each DAG consists of the following general stages:

1. Create cluster (if it already exists, this step is skipped)

1. Install dependencies on the cluster

1. Run data processing steps for this DAG

1. Delete the cluster

Within a DAG, all data processing steps run on the same Dataproc cluster as separate jobs.

There is no need to configure DAGs or steps depending on the size of the input data. Clusters have autoscaling enabled, which means they will increase or decrease the number of worker VMs to accommodate the load.

## DAG 1: Preprocess

This DAG contains steps which are only supposed to be run once, or very rarely. They ingest external data and apply bespoke transformations specific for each particular data source. The output is normalised according to the data schemas used by the pipeline.

## DAG 2: ETL

The ETL DAG takes the inputs of the previous step and performs the main algorithmic processing. This processing is supposed to be data source agnostic.
