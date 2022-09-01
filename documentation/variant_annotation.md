# Variant annotation pipeline

Workflow to produce variant annotation dataset for Open Targets Genetics Portal. The dataset is derived from the GnomAD 3.1 release, whith some modification. This dataset is used to generate annotation for all the variants the Portal has association information on. Also the variant index is derived from this dataset.

In this release of the variant annotation pipeline there is no allele frequency or variant call quality filter.

Steps:

- Lifts over to GRCh37 for historic reason.
- Keep VEP annotations including regulatory, motif features and transcript features.
- Transcript consequences are filtered for only canonical transcipts.

## Usage

For detailed description and the applied defaults of the script, please run `generate_variant_annotation.py -h`.

### Start dataproc spark server

The dataproc cluster is single node, high-mem, with the most recent version of hail (`v.0.2.77`) as of 2021-10-05.

```bash
gcloud dataproc clusters create hail-test-single \
    --image-version=2.0.6-debian10 \
    --properties="^|||^spark:spark.task.maxFailures=20|||spark:spark.driver.extraJavaOptions=-Xss4M|||spark:spark.executor.extraJavaOptions=-Xss4M|||hdfs:dfs.replication=1|||spark:spark.driver.memory=1146g" \
    --initialization-actions="gs://hail-common/hailctl/dataproc/0.2.77/init_notebook.py" \
    --metadata="^|||^WHEEL=gs://hail-common/hailctl/dataproc/0.2.77/hail-0.2.77-py3-none-any.whl|||PKGS=aiohttp==3.7.4|aiohttp_session>=2.7,<2.8|asyncinit>=0.2.4,<0.3|avro>=1.10,<1.11|azure-identity==1.6.0|azure-storage-blob==12.8.1|bokeh>1.3,<2.0|boto3>=1.17,<2.0|botocore>=1.20,<2.0|decorator<5|Deprecated>=1.2.10,<1.3|dill>=0.3.1.1,<0.4|gcsfs==0.8.0|fsspec==0.9.0|google-auth==1.27.0|humanize==1.0.0|hurry.filesize==0.9|janus>=0.6,<0.7|nest_asyncio|numpy<2|pandas>=1.1.0,<1.1.5|parsimonious<0.9|PyJWT|python-json-logger==0.1.11|requests==2.25.1|scipy>1.2,<1.7|sortedcontainers==2.1.0|tabulate==0.8.3|tqdm==4.42.1|google-cloud-storage==1.25.*" \
    --master-machine-type=m1-megamem-96 \
    --master-boot-disk-size=100GB \
    --secondary-worker-boot-disk-size=40GB \
    --worker-boot-disk-size=40GB \
    --single-node \
    --worker-machine-type=n1-standard-8 \
    --region=europe-west1 \
    --initialization-action-timeout=20m \
    --project=open-targets-genetics-dev
```

### Submit job to dataproc server

```bash
CLUSTER="hail-test-single"
PROJECT="open-targets-genetics-dev"
REGION="europe-west1"
TODAY=$(date "+%Y-%m-%d")

gcloud dataproc jobs submit pyspark \
  --cluster=${CLUSTER} \
  --project=${PROJECT} \
  --region=${REGION} \
  generate_variant_annotation.py -- \
  --outputFolder gs://genetics-portal-dev-raw/variant_index/${TODAY}
```

## Output schema

```schema
root
 |-- locus: struct (nullable = true)
 |    |-- contig: string (nullable = true)
 |    |-- position: integer (nullable = true)
 |-- alleles: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- locus_GRCh38: struct (nullable = true)
 |    |-- contig: string (nullable = true)
 |    |-- position: integer (nullable = true)
 |-- chrom_b38: string (nullable = true)
 |-- pos_b38: integer (nullable = true)
 |-- chrom_b37: string (nullable = true)
 |-- pos_b37: integer (nullable = true)
 |-- ref: string (nullable = true)
 |-- alt: string (nullable = true)
 |-- allele_type: string (nullable = true)
 |-- vep: struct (nullable = true)
 |    |-- most_severe_consequence: string (nullable = true)
 |    |-- motif_feature_consequences: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- allele_num: integer (nullable = true)
 |    |    |    |-- consequence_terms: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- high_inf_pos: string (nullable = true)
 |    |    |    |-- impact: string (nullable = true)
 |    |    |    |-- minimised: integer (nullable = true)
 |    |    |    |-- motif_feature_id: string (nullable = true)
 |    |    |    |-- motif_name: string (nullable = true)
 |    |    |    |-- motif_pos: integer (nullable = true)
 |    |    |    |-- motif_score_change: double (nullable = true)
 |    |    |    |-- strand: integer (nullable = true)
 |    |    |    |-- variant_allele: string (nullable = true)
 |    |-- regulatory_feature_consequences: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- allele_num: integer (nullable = true)
 |    |    |    |-- biotype: string (nullable = true)
 |    |    |    |-- consequence_terms: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- impact: string (nullable = true)
 |    |    |    |-- minimised: integer (nullable = true)
 |    |    |    |-- regulatory_feature_id: string (nullable = true)
 |    |    |    |-- variant_allele: string (nullable = true)
 |    |-- transcript_consequences: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- allele_num: integer (nullable = true)
 |    |    |    |-- amino_acids: string (nullable = true)
 |    |    |    |-- appris: string (nullable = true)
 |    |    |    |-- biotype: string (nullable = true)
 |    |    |    |-- canonical: integer (nullable = true)
 |    |    |    |-- ccds: string (nullable = true)
 |    |    |    |-- cdna_start: integer (nullable = true)
 |    |    |    |-- cdna_end: integer (nullable = true)
 |    |    |    |-- cds_end: integer (nullable = true)
 |    |    |    |-- cds_start: integer (nullable = true)
 |    |    |    |-- codons: string (nullable = true)
 |    |    |    |-- consequence_terms: array (nullable = true)
 |    |    |    |    |-- element: string (containsNull = true)
 |    |    |    |-- distance: integer (nullable = true)
 |    |    |    |-- domains: array (nullable = true)
 |    |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |    |-- db: string (nullable = true)
 |    |    |    |    |    |-- name: string (nullable = true)
 |    |    |    |-- exon: string (nullable = true)
 |    |    |    |-- gene_id: string (nullable = true)
 |    |    |    |-- gene_pheno: integer (nullable = true)
 |    |    |    |-- gene_symbol: string (nullable = true)
 |    |    |    |-- gene_symbol_source: string (nullable = true)
 |    |    |    |-- hgnc_id: string (nullable = true)
 |    |    |    |-- hgvsc: string (nullable = true)
 |    |    |    |-- hgvsp: string (nullable = true)
 |    |    |    |-- hgvs_offset: integer (nullable = true)
 |    |    |    |-- impact: string (nullable = true)
 |    |    |    |-- intron: string (nullable = true)
 |    |    |    |-- lof: string (nullable = true)
 |    |    |    |-- lof_flags: string (nullable = true)
 |    |    |    |-- lof_filter: string (nullable = true)
 |    |    |    |-- lof_info: string (nullable = true)
 |    |    |    |-- minimised: integer (nullable = true)
 |    |    |    |-- polyphen_prediction: string (nullable = true)
 |    |    |    |-- polyphen_score: double (nullable = true)
 |    |    |    |-- protein_end: integer (nullable = true)
 |    |    |    |-- protein_start: integer (nullable = true)
 |    |    |    |-- protein_id: string (nullable = true)
 |    |    |    |-- sift_prediction: string (nullable = true)
 |    |    |    |-- sift_score: double (nullable = true)
 |    |    |    |-- strand: integer (nullable = true)
 |    |    |    |-- swissprot: string (nullable = true)
 |    |    |    |-- transcript_id: string (nullable = true)
 |    |    |    |-- trembl: string (nullable = true)
 |    |    |    |-- tsl: integer (nullable = true)
 |    |    |    |-- uniparc: string (nullable = true)
 |    |    |    |-- variant_allele: string (nullable = true)
 |-- rsid: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- af: struct (nullable = true)
 |    |-- oth: double (nullable = true)
 |    |-- amr: double (nullable = true)
 |    |-- fin: double (nullable = true)
 |    |-- ami: double (nullable = true)
 |    |-- mid: double (nullable = true)
 |    |-- nfe: double (nullable = true)
 |    |-- sas: double (nullable = true)
 |    |-- asj: double (nullable = true)
 |    |-- eas: double (nullable = true)
 |    |-- afr: double (nullable = true)
 |-- cadd: struct (nullable = true)
 |    |-- phred: float (nullable = true)
 |    |-- raw: float (nullable = true)
```
