---
title: Complex Portal
---

[Complex Portal](https://www.ebi.ac.uk/complexportal/) is a manually curated resource of macromolecular complexes maintained by EMBL-EBI. It provides two complementary datasets:

- **Experimental** – complexes with direct experimental evidence.
- **Predicted** – computationally predicted complexes.

Both files are distributed in the **ComplexTAB** flat-file format and are filtered to human complexes (NCBI taxonomy ID 9606) during ingestion.

The resulting `MolecularComplex` dataset is used downstream in the deCODE proteomics pipeline to annotate multi-protein SomaScan aptamers with a `molecularComplexId`.

::: gentropy.datasource.complex_portal.ComplexTab
