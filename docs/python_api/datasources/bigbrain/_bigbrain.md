---
title: BigBrain brain molecular QTLs
---

[BigBrain](https://zenodo.org/records/17226890) ([Réal et al., 2025](https://pubmed.ncbi.nlm.nih.gov/41040686/)) is a multi-cohort brain-tissue molecular-QTL
meta-analysis (43 tissue-cohort pairs, 10,725 samples from 4,656 donors), mapped using the
mmQTL random-effects meta-analysis method ([Zeng et al., 2022](https://pubmed.ncbi.nlm.nih.gov/35058635/)).

Two sub-datasets are provided. Open Targets ingests BigBrain's own published fine-mapping
for the EUR-ancestry release of both, as credible sets with a matching study index:

- **eQTL** (`BigBrain_eqtl_EUR_{gene}`): cis-eQTL associations, one study per Ensembl gene.
  Source: [Zenodo record 17226890](https://zenodo.org/records/17226890).
- **sQTL** (`BigBrain_sqtl_EUR_{junction}`): cis-sQTL associations, one study per
  Leafcutter intron junction. Source: [Zenodo record 17153730](https://zenodo.org/records/17153730).

!!! note "Credible sets come from BigBrain's own fine-mapping"
BigBrain fine-mapped with PolyFun + SuSiE using in-sample LD from 3,884 European-ancestry
donors. Its published credible sets are ingested directly (`BigBrainFinemapping`) rather
than re-fine-mapped against a reference LD panel. They carry PIPs but no log Bayes factors,
so they suit PIP-based colocalisation (`coloc_pip`, `ecaviar`).

!!! note "Study sample size"
The study index records the total sample size reported for the whole resource (10,725
samples). Credible sets carry the sample size of their lead variant (up to 9,310 in EUR).

::: gentropy.datasource.bigbrain.BigBrainPublicationMetadata
