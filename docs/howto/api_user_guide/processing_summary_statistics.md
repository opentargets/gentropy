---
title: Processing summary statistics
---

Summary statistics are the result of a genome-wide association study (GWAS). They are a collection of statistics for each variant tested in the study. The statistics can include the effect size, the standard error, the p-value, the allele frequency, and the number of samples tested.

Gentropy provides a datatype to represent summary statistics, called `SummaryStatistics`. On this datatype you can perform useful operations that are useful to disentangle the genetic architecture of a trait or disease.

## Creating a SummaryStatistics object

To begin working with GWAS summary statistics, you first need to load them into Gentropy. This typically involves reading from a collection of parquet files containing single-point statistics from a study. The SummaryStatistics class in Gentropy is designed to facilitate this process.

```python
from gentropy.datasets.summary_statistics import SummaryStatistics

# Specify the path to your summary statistics data
path = "path/to/summary/stats"

# Create a SummaryStatistics object by loading data from the specified path
summary_stats = SummaryStatistics.from_parquet(session, path)
```

By executing the above code, you create a SummaryStatistics object, loading data from the provided path, ready for further analysis.

## Identification of independent association signals

A fundamental step in dissecting the genetic architecture of traits or diseases is to identify independent association signals. These signals are discerned by clumping associations, a process that groups nearby variants based on their physical distance and significance levels, thus isolating top hits or primary variants.

Here’s how to perform window-based clumping in Gentropy:

```python
# Perform window-based clumping on summary statistics
# By default, the method uses a 1Mb window and a p-value threshold of 5e-8
clumped_summary_statistics = summary_stats.window_based_clumping()
```

## What's next?

The result of clumping a summary statistics dataset is a `StudyLocus` dataset, which is a datatype that represents the association between a specific genome region and a trait.
Now that you have a more fine-grained representation of the genetic architecture of your trait or disease, in the next section you'll see how to identify the most likely causal SNPs in your genomic regions.
