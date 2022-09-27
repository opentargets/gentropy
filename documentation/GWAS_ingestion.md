# Ingesting GWAS Catalog curated data

This pipeline ingest the curated GWAS Catalog associations and studies. Prepares top-loci table and study table.

## Summary of the logic

1. Reading association data set. Applying some filters and do basic processing.
2. Map associated variants to GnomAD variants based on the annotated chromosome:position (on GRCh38).
3. Harmonize effect size, calucate Z-score + direction of effect.
4. Read and process study table (parse ancestry, sample size etc).
5. Join study table with associations on `study_accession`. Split studies when necessary on the basis that one study describes one trait only.
6. Split data into top-loci and study tables and save.
