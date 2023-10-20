<p align="center">
  <img width="400" height="200" src="../../../../assets/imgs/open_targets_platform.svg">
</p>
<style>
  .md-typeset h1,
  .md-content__button {
    display: none;
  }
</style>

The Open Targets Platform is a comprehensive resource that aims to aggregate and harmonise various types of data to facilitate the identification, prioritisation, and validation of drug targets. Genomic data from Open Targets provides gene identification and genomic coordinates that are integrated into the gene index of our ETL pipeline.

The EMBL-EBI Ensembl database is used as a source for human targets in the Platform, with the Ensembl gene ID as the primary identifier. The criteria for target inclusion is:
- Genes from all biotypes encoded in canonical chromosomes
- Genes in alternative assemblies encoding for a reviewed protein product.
