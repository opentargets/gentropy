---
title: QC of GWAS Summary Statistics
---

This class consists of several general quality control checks for GWAS with full summary statistics.
There are several checks included:

1. Genomic control lambda (median of the distribution of Chi2 statistics divided by expected for Chi2 with df=1). Lambda should be reasonably close to 1. Ideally not bigger than 2.

2. P-Z check: the linear regression between log10 of reported p-values and log10 of p-values inferred from betas and standard errors. Intercept of the regression should be close to 0, slope close to 1.

3. Mean beta check: mean of beta. Should be close to 0.

4. The N_eff check: It estimates the ratio between effective sample size and the expected one and checks its distribution. It is possible to conduct only if the effective allele frequency is provided in the study. The median ratio is always close to 1, standard error should be close to 0.

5. Number of SNPs and number of significant SNPs.

## Summary Statistics QC checks

:::gentropy.method.sumstat_quality_controls.gc_lambda_check
:::gentropy.method.sumstat_quality_controls.p_z_test
:::gentropy.method.sumstat_quality_controls.mean_beta_check
:::gentropy.method.sumstat_quality_controls.sumstat_n_eff_check
:::gentropy.method.sumstat_quality_controls.number_of_variants
