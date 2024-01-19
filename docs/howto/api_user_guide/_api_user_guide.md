---
title: First steps with Gentropy
---

Welcome to the "First Steps" tutorial for Gentropy, the innovative Python package developed by Open Targets. This tutorial series is designed to guide you through the powerful features and capabilities of Gentropy, providing a foundational understanding of how to leverage it for your genetics analyses.

## What is Gentropy?

Gentropy is a cutting-edge Python package, specifically designed to facilitate efficient and user-friendly genetics analyses. At its core, Gentropy offers a robust framework for handling common datatypes in genetics research. This package is crafted with performance and ease-of-use in mind, making complex genetics analyses more accessible and streamlined.

## Key Features:

- **Specialized Datatypes**: Gentropy introduces a set of unique datatypes that are essential in the field of genetics. These include:

  - _StudyLocus_: Represents the association between a specific genome region and a trait.
  - _LocusToGene_: Maps the relationship between loci (usually non-coding) and underlying causal genes.
  - _SummaryStatistics_: Represents single point statistics resulting from a study.
  - ... More in the [data sources section](python_api/datasets/_datasets.md).

- **Performance-Oriented**: Built with performance as a priority, Gentropy is optimized to handle large-scale genetics data efficiently, ensuring quick and accurate analyses. These include:

  - _Locus to gene scores_. Prioritize likely causal genes at each GWAS locus.
  - _Fine mapping_. Identify the most likely causal SNPs associated with a trait or disease within a genomic region.
  - _Colocalisation_. Identify whether two traits share a common causal variant.
  - More in the [methods section](python_api/methods/_methods.md)

- **User-Friendly**: The package is designed to be intuitive, allowing both beginners and experienced researchers to conduct complex genetic analyses with ease.

By the end of this tutorial series, you will have a basic understanding of Gentropy and how it can revolutionize your approach to genetics analyses. Whether you're a researcher, a student, or a professional in the field, Gentropy is set to become an indispensable tool in your arsenal.
