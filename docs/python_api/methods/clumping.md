---
title: Clumping
---

# Clumping

Clumping is a commonly used post-processing method that allows for the identification of independent association signals from GWAS summary statistics and curated associations. This process is critical because of the complex linkage disequilibrium (LD) structure in human populations, which can result in multiple statistically significant associations within the same genomic region. Clumping methods help reduce redundancy in GWAS results and ensure that each reported association represents an independent signal.

We have implemented three clumping methods:

1. **Window clumping:** Uses a genomic window around each lead to clump the significant variants into one locus.
2. **LD clumping:** Flags or drops study loci whose lead is in LD with a more significant lead.
3. **Locus-breaker clumping:** Applies a distance cutoff between baseline significant variants, and returns one locus per block.

The algorithmic logic is similar to classic clumping approaches from PLINK (Reference: [PLINK Clump Documentation](https://zzz.bwh.harvard.edu/plink/clump.shtml)). See details below:

# Window clumping

::: gentropy.method.clumping.window

# LD clumping

::: gentropy.method.clumping.ld

# Locus-breaker clumping

::: gentropy.method.clumping.locus_breaker
