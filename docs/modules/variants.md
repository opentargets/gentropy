This workflow produces two outputs from the variant dataset of GnomAD.

1. Variant annotation. The dataset is derived from the GnomAD 3.1 release, with some modification. This dataset is used in other pipelines to generate annotation for all the variants the Portal processes.
2. Variant index. Based on the variant annotation dataset, the dataset has been filtered to only contain variants that have association data.

Schemas for each dataset are defined in the `json.schemas` module.

## Summary of the logic

### Variant annotation
1. The variant dataset from GnomAD is processed with Hail to extract relevant information about a variant.
2. The transcript consequences features provided by VEP are filtered to only refer to only refer to the canonical transcript.
3. Genome coordinates are liftovered from GRCh38 to GRCh37.
4. Field names are converted to camel case, to follow the same conventions as other pipelines.

::: etl.variants.variant_annotation
### Variant index
1. The variant annotation dataset is further processed to follow our variant model definition.
2. The dataset is filtered to only include variants that are present in the credible set. The variants in the credible set that are filtered out are written in the invalid variants file.

::: etl.variants.variant_index
