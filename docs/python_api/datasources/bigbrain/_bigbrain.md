---
title: BigBrain brain molecular QTLs
---

[BigBrain](https://zenodo.org/records/17226890) is a multi-cohort brain-tissue molecular-QTL
meta-analysis (43 tissue-cohort pairs, 10,725 samples from 4,656 donors), mapped using the
mmQTL random-effects meta-analysis method ([Zeng et al., 2022](https://pubmed.ncbi.nlm.nih.gov/35058635/)).

Two sub-datasets are provided (this ingestion covers the EUR-ancestry release of both):

- **EQTL** (`BigBrain-eqtl-EUR`): cis-eQTL associations, one study per Ensembl gene.
  Source: [Zenodo record 17226890](https://zenodo.org/records/17226890).
- **SQTL** (`BigBrain-sqtl-EUR`): cis-sQTL associations, one study per Leafcutter
  intron-cluster feature. Source: [Zenodo record 17153730](https://zenodo.org/records/17153730).

!!! note "Backfilled sample size, no per-variant allele frequency"
The source reports no per-variant sample size or allele frequency. `sampleSize` is
backfilled with the constant total sample size reported for the whole resource
(10,725 donors, shared by eQTL and sQTL); `effectAlleleFrequencyFromSource` is left null.

::: gentropy.datasource.bigbrain.BigBrainQtlType
::: gentropy.datasource.bigbrain.BigBrainPublicationMetadata
