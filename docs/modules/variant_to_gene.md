All variants in the variant index are annotated using our Variant-to-Gene (V2G) pipeline. The pipeline integrates V2G evidence that fall into four main data types:

1. Molecular phenotype quantitative trait loci experiments (eQTLs, pQTLs and sQTLs).
2. Chromatin interaction experiments, e.g. Promoter Capture Hi-C (PCHi-C).
3. In silico functional predictions, e.g. Variant Effect Predictor (VEP) from Ensembl.
4. Distance between the variant and each gene's canonical transcription start site (TSS).

Within each data type there are multiple sources of information produced by different experimental methods. Some of these sources can further be broken down into separate tissues or cell types (features).

## Summary of the logic

### Distance to the canonical TSS

::: etl.v2g.distance.distance
