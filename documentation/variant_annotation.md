# Generating variant annotation

This workflow produce variant annotation dataset for Open Targets Genetics Portal. The dataset is derived from the GnomAD 3.1 release, whith some modification. This dataset is used to generate annotation for all the variants the Portal, which has association information.

**Important**: in this release of the variant annotation pipeline there is **no** allele frequency or variant call quality filter.

Steps:

- Lifts over to GRCh37 for historic reason.
- Keep VEP annotations including regulatory, motif features and transcript features.
- Transcript consequences are filtered for only canonical transcipts.

## Usage

All parameters required for the run are read from the configuration provided by hydra.

```bash
python generate_variant_annotation.py
```

## Output schema

The output is partitioned by chromosome and saved in parquet format.

```schema
root
 |-- id: string (nullable = true)
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
