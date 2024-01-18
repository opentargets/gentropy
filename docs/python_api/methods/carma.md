---
title: CARMA
---

CARMA is the method of the fine-mapping and outlier detection, originally implemented in R ([CARMA on GitHub](https://github.com/ZikunY/CARMA)).

The full repository for the reimplementation of CARMA in Python can be found [here](https://github.com/hlnicholls/carmapy/tree/0.1.0).

This is a simplified version of CARMA with the following features:

1. It uses only Spike-slab effect size priors and Poisson model priors.
2. C++ is re-implemented in Python.
3. The way of storing the configuration list is changed. It uses a string with the list of indexes for causal SNPs instead of a sparse matrix.
4. Fixed bugs in PIP calculation.
5. No credible models.
6. No credible sets, only PIPs.
7. No functional annotations.
8. Removed unnecessary parameters.

:::gentropy.method.carma.CARMA
