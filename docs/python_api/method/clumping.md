---
title: Clumping
---

# Clumping

Clumping is a commonly used post-processing method that allows for identification of independent association signals from GWAS summary statistics and curated associations. This process is critical because of the complex linkage disequilibrium (LD) structure in human populations, which can result in multiple statistically significant associations within the same genomic region. Clumping methods help reduce redundancy in GWAS results and ensure that each reported association represents an independent signal.

We have implemented 2 clumping methods:

::: oxygen.method.clump.LDclumping
