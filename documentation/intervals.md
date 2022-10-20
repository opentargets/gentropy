# Generating the intervals dataset

This workflow produce intervals dataset that links genes to genomic regions based on genome interaction studies.

variant annotation dataset for Open Targets Genetics Portal. The dataset is derived from the GnomAD 3.1 release, whith some modification. This dataset is used to generate annotation for all the variants the Portal, which has association information.

## Ingested studies

### 1. Andersson *et al* 2014

As part of the [FANTOM5](https://fantom.gsc.riken.jp/5/) genome mapping effort, this publication aims to report on actively transcribed enhancers from the majority of human tissues. ([Link](https://www.nature.com/articles/nature12787) to the publication)

The dataset is not allows to resolve individual tissues, the biotype is `aggregate`. However the aggregation provides a score quantifying the association of the genomic region and the gene.

### 2. Javierre *et al* 2016

Javierre and colleagues uses promoter capture Hi-C to identify interacting regions of 31,253 promoters in 17 human primary hematopoietic cell types. ([Link](https://www.sciencedirect.com/science/article/pii/S0092867416313228) to the publication)

The dataset provides cell type resolution, however these cell types are not resolved to tissues. Scores are also preserved.

### 3. Jung *et al* 2019

Promoter capture Hi-C was used to map long-range chromatin interactions for 18,943 well-annotated promoters for protein-coding genes in 27 human tissue types. ([Link](https://www.nature.com/articles/s41588-019-0494-8) to the publication)

This dataset provides tissue level annotation, but no cell type or biofeature is given. Also scores are not provided.

### 4. Thurman *et al* 2014

In this projet cis regulatory elements were mapped using DNase I hypersensitive site (DHSs) mapping. ([Link](https://www.nature.com/articles/nature11232) to the publication)

This is also an aggregated dataset, so no cellular or tissue annotation is preserved, however scores are given.

## Usage

All parameters required for the run are read from the configuration provided by hydra.

```bash
python run_intervals.py
```

## Output schema

The output is saved in parquet format.

```schema
root
 |-- chrom: string (nullable = true)
 |-- start: string (nullable = true)
 |-- end: string (nullable = true)
 |-- gene_id: string (nullable = true)
 |-- score: double (nullable = true)
 |-- dataset_name: string (nullable = true)
 |-- data_type: string (nullable = true)
 |-- experiment_type: string (nullable = true)
 |-- pmid: string (nullable = true)
 |-- bio_feature: string (nullable = true)
 |-- cell_type: string (nullable = true)
 |-- tissue: string (nullable = true)
```

While genomic location and the gene identifier are always present, some fields are optional depending on the data the given project provides.
