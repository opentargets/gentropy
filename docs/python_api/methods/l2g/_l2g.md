---
title: Locus to Gene (L2G) model
---

The **“locus-to-gene” (L2G)** model derives features to prioritize likely causal genes at each GWAS locus based on genetic and functional genomics features. The main categories of predictive features are:

- **Distance:** (from credible set variants to gene)
- **Molecular QTL Colocalization**
- **Chromatin Interaction:** (e.g., promoter-capture Hi-C)
- **Variant Pathogenicity:** (from VEP)

Some of the predictive features weight variant-to-gene (or genomic region-to-gene) evidence based on the posterior probability that the variant is causal, determined through fine-mapping of the GWAS association.

For a more detailed description of how each feature is computed, see [the L2G Feature documentation](../../datasets/l2g_features/_l2g_feature.md).

Details of the L2G model are provided in our Nature Genetics publication (ref - [Nature Genetics Publication](https://www.nature.com/articles/s41588-021-00945-5)):

- **Title:** An open approach to systematically prioritize causal variants and genes at all published human GWAS trait-associated loci.
- **Authors:** Mountjoy, E., Schmidt, E.M., Carmona, M. et al.
- **Journal:** Nat Genet 53, 1527–1533 (2021).
- **DOI:** [10.1038/s41588-021-00945-5](https://doi.org/10.1038/s41588-021-00945-5)
