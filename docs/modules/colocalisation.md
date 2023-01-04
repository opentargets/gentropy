This workflow runs colocalization analyses that assess the degree to which independent signals of the association share the same causal variant in a region of the genome, typically limited by linkage disequilibrium (LD).

The colocalisation test is performed using two methods:

1. Based on the [R COLOC package](https://github.com/chr1swallace/coloc/blob/main/R/claudia.R), which uses the Bayes factors from the credible set to estimate the posterior probability of colocalisation. This method makes the simplifying assumption that **only a single causal variant** exists for any given trait in any genomic region.

    Using GWAS summary statistics, and without information about LD, we start by enumerating all variant-level hypotheses:

    Hypothesis | Description
    --- | ---
    H_0 | no association with either trait in the region
    H_1 | association with trait 1 only
    H_2 | association with trait 2 only
    H_3 | both traits are associated, but have different single causal variants
    H_4 | both traits are associated and share the same single causal variant



2. Based on eCAVIAR. It extends the [CAVIAR](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5142122/#bib18) framework to explicitly estimate the posterior probability that the same variant is causal in 2 studies while accounting for the uncertainty of LD.

    eCAVIAR computes the colocalization posterior probability (**CLPP**) by utilizing the marginal posterior probabilities derived from PICS. This framework allows for **multiple variants to be causal** in a single locus.

## Summary of the logic

The workflow is divided into 2 steps for both methods:

**1. Find all vs all pairs of independent signals of association in the region of interest.**
::: etl.coloc.overlaps

**2. For each pair of signals, run the colocalisation test.**
::: etl.coloc.coloc
