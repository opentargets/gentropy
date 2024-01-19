---
title: Finemapping
---

The `StudyLocus` dataset represents primary signals found to be statistically associated in a study. Starting from there, you can perform fine mapping to measure the probability that each of those signals are truly causal.

Gentropy currently supports two fine mapping methods that are based on Bayesian statistics: [PICS](python_api/methods/pics.md) and [CARMA](python_api/methods/carma.md).

## Fine-mapping with PICS

### Expand each locus to a set of SNPs

The power of fine-mapping lies in expanding our understanding of each StudyLocus, going beyond the lead SNP to encompass a broader set of SNPs in linkage disequilibrium (LD) with it.

To facilitate this, we utilize a LD Index, a comprehensive dataset detailing the LD structure across various populations.

```python
from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
# Load the LD Index and the Study Index to extract population structure information
ld_path = "path/to/ld_index"
studies_path = "path/to/study_index"
ld_index = LDIndex.from_parquet(session, ld_index_path)
study_index = StudyIndex.from_parquet(session, study_index_path, recursiveFileLookup=True)

# Annotate the StudyLocus dataset with LD information
study_locus_ld_annotated = study_locus.annotate_ld(
                study_index=study_index, ld_index=ld_index
            )
```

By annotating the StudyLocus dataset with LD information, we set the stage for a more accurate and comprehensive fine-mapping analysis.

### Define credible sets

Defining credible sets is a critical step in fine-mapping, where we assign posterior probabilities to the set of SNPs in LD with the lead SNP of each locus. This process allows us to quantify the likelihood of each SNP being causal for the trait or disease under study. By establishing these credible sets, we can pinpoint the most likely causal variants within each locus, providing valuable insights into the genetic underpinnings of the trait or disease.

Here's how to perform this step using the PICS method:

```python
from gentropy.method.pics import PICS

finemapped_study_locus = PICS.finemap(study_locus_ld_annotated).annotate_credible_sets()
```

## What's next?

The result of fine-mapping a StudyLocus dataset is another `StudyLocus` dataset, with a more defined representation of the genetic architecture of your trait or disease. This is useful to extract functional genomics information and prioritise genes at each locus using the LocusToGene algorithm.
