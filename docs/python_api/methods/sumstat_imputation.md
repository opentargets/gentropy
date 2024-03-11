---
title: Summary Statistics Imputation
---

Summary statistics imputation leverages linkage disequilibrium (LD) information to compute Z-scores of missing SNPs from neighbouring observed SNPs
SNPs by taking advantage of the Linkage Disequilibrium.

We implemented the basic model from RAISS (Robust and Accurate Imputation from Summary Statistics) package (see the original [paper](https://academic.oup.com/bioinformatics/article/35/22/4837/5512360)).

The full repository for the RAISS package can be found [here](https://gitlab.pasteur.fr/statistical-genetics/raiss).

The original model was suggested in 2014 by Bogdan Pasaniuc et al. [here](https://pubmed.ncbi.nlm.nih.gov/24990607/).

It represents the following formula:

E(z*i|z_t) = M*{i,t} \cdot (M\_{t,t})^{-1} \cdot z_t

Where:

- E(z_i|z_t) represents the expected z-score of SNP 'i' given the observed z-scores at known SNP indexes 't'.

- M\_{i,t} represents the LD (Linkage Disequilibrium) matrix between SNP 'i' and the known SNPs at indexes 't'.

- (M\_{t,t})^{-1} represents the inverse of the LD matrix of the known SNPs at indexes 't'.

- z_t represents the vector of observed z-scores at the known SNP indexes 't'.

:::gentropy.method.sumstat_imputation.SummaryStatisticsImputation
