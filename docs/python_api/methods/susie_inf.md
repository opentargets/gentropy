---
title: SuSiE-inf
---

# SuSiE-inf - Fine-mapping with infinitesimal effects v1.1

This is an implementation of the SuSiE-inf method found here:
https://github.com/FinucaneLab/fine-mapping-inf
https://www.nature.com/articles/s41588-023-01597-3

This fine-mapping approach has two approaches for updating estimates of the variance components - Method of Moments and Maximum Likelihood Estimator ('MoM' / 'MLE')
The function takes an array of Z-scores and a numpy array matrix of variant LD to perform finemapping.

:::gentropy.method.susie_inf.SUSIE_inf
