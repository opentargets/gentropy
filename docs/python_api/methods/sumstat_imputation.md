---
title: Summary Statistics Imputation
---

Summary statistics imputation enables the imputation of SNP summary statistics from the neighboring
SNPs by taking advantage of the Linkage Disequilibrium.

We implemented the basic model from RAISS (Robust and Accurate Imputation from Summary Statistics) package (see the original [paper](https://academic.oup.com/bioinformatics/article/35/22/4837/5512360)).

The full repository for the RAISS package can be found [here](https://gitlab.pasteur.fr/statistical-genetics/raiss).

The original model was suggested in 2014 by Bogdan Pasaniuc et. al. [here](https://pubmed.ncbi.nlm.nih.gov/24990607/).

:::gentropy.method.sumstat_imputation.sumstat_imputation
