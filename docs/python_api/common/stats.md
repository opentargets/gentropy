---
title: stats
---

## Statistical methods used for GWAS processing

The functions below are used during the harmonisation of summary statistics effect size, p-value and confidence intervals, standard error calculations.

NOTE: Due to low p-value values, the functions work with pvalue in one of two formats:

- as negative log10 p-value (neglogoval)
- as mantissa and exponent (2 separate columns)

:::gentropy.common.stats
