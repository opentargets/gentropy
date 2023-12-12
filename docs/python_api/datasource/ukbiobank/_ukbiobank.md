---
title: UK Biobank
---

<p align="center">
  <img width="400" height="200" src="../../../../assets/imgs/UK_biobank_logo.png">
</p>
<style>
  .md-typeset h1,
  .md-content__button {
    display: none;
  }
</style>

The UK Biobank is a large-scale biomedical database and research resource that contains a diverse range of in-depth information from 500,000 volunteers in the United Kingdom. Its genomic data comprises whole-genome sequencing for a subset of participants, along with genotyping arrays for the entire cohort. The data has been a cornerstone for numerous genome-wide association studies (GWAS) and other genetic analyses, advancing our understanding of human health and disease.

Recent efforts to rapidly and systematically apply established GWAS methods to all available data fields in UK Biobank have made available large repositories of summary statistics. To leverage these data disease locus discovery, we used full summary statistics from:
The Neale lab Round 2 (N=2139).

- These analyses applied GWAS (implemented in Hail) to all data fields using imputed genotypes from HRC as released by UK Biobank in May 2017, consisting of 337,199 individuals post-QC. Full details of the Neale lab GWAS implementation are available here. We have remove all ICD-10 related traits from the Neale data to reduce overlap with the SAIGE results.
- http://www.nealelab.is/uk-biobank/
  The University of Michigan SAIGE analysis (N=1281).
- The SAIGE analysis uses PheCode derived phenotypes and applies a new method that "provides accurate P values even when case-control ratios are extremely unbalanced". See Zhou et al. (2018) for further details.
- https://pubmed.ncbi.nlm.nih.gov/30104761/
