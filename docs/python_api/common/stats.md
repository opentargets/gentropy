---
title: stats
---

## Statistical methods used for GWAS processing

The functions below are used during the harmonisation of summary statistics effect size, p-value and confidence intervals, standard error calculations.

NOTE: Due to low p-value values, the functions work with pvalue in one of two formats:

- as negative log10 p-value (neglogoval)
- as mantissa and exponent (2 separate columns)

:::gentropy.common.stats
:::gentropy.common.stats.get_logsum
:::gentropy.common.stats.split_pvalue
:::gentropy.common.stats.chi2_from_pvalue
:::gentropy.common.stats.ci
:::gentropy.common.stats.neglogpval_from_z2
:::gentropy.common.stats.neglogpval_from_pvalue
:::gentropy.common.stats.normalise_gwas_statistics
:::gentropy.common.stats.pvalue_from_neglogpval
:::gentropy.common.stats.split_pvalue_column
:::gentropy.common.stats.stderr_from_chi2_and_effect_size
:::gentropy.common.stats.stderr_from_ci
:::gentropy.common.stats.zscore_from_pvalue
