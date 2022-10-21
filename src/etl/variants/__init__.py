
"""This module contains logic to manage variant related logic. pipeline the GnomAD3.1 dataset and generates variant annotation dataset.

## Components

### 1. Generating variant annotation dataset

`variant annotation` dataset for Open Targets Genetics Portal. The dataset is derived from the GnomAD 3.1 release, whith minor modification. This dataset is used to generate annotation for all the variants the Portal, which has association information.
**Important**: in this release of the variant annotation pipeline there is **no** allele frequency or variant call quality filter.

Steps:

1. The variant dataset from GnomAD is processed with Hail to extract relevant information about a variant.
2. The transcript consequences features provided by VEP are filtered to only refer to only refer to the canonical transcript.
3. Genome coordinates are liftovered from GRCh38 to GRCh37.
4. Field names are converted to camel case, to follow the same conventions as other pipelines.


### 2. Generating variant index

1. The variant annotation dataset is further processed to follow our variant model definition.
2. The dataset is filtered to only include variants that are present in the credible set. The variants in the credible set that are filtered out are written in the invalid variants file.

::: etl.variants
::: etl.variants.variant_index
    :members:
    :show-inheritance:
"""

from __future__ import annotations
