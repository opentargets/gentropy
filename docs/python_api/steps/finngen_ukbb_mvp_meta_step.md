---
title: FinnGen UKBB MVP Meta Analysis ingestion
---

## Pipeline overview

The ingestion pipeline consists of four steps that must be executed in order:

1. **`FinngenUkbMvpMetaStudyIndexStep`** — builds `StudyIndex` from the manifest and EFO curation.
2. **`FinngenUkbMvpMetaSumstatConversionStep`** — converts BGZIP summary statistics to Parquet, partitioned by `studyId`.
3. **`FinngenUkbMvpMetaSumstatHarmonisationStep`** — harmonises summary statistics using gnomAD allele directions.
4. **`FinngenUkbMvpMetaStudyIndexQCAnnotationStep`** — runs summary-statistics QC and annotates the study index.

`FinngenUkbMvpMetaSummaryStatisticsIngestionStep` is a convenience façade that chains all four steps.

```mermaid
graph TD
    %% --- INPUTS ---
    A1([source_manifest_path]) --> B1
    A2([efo_curation_path]) --> B2
    A3([finngen_release]) --> C1
    A4([gnomad_variant_index_path]) --> G1
    A5([Source Summary Statistics BGZIP]) --> C3

    %% --- STEP 1: StudyIndex ---
    subgraph S1["① FinngenUkbMvpMetaStudyIndexStep"]
        B1["FinnGenMetaManifest"] --> C1["StudyIndex"]
        B2["EFOMapping"] --> C1
    end

    %% --- STEP 2: BGZIP to Parquet ---
    subgraph S2["② FinngenUkbMvpMetaSumstatConversionStep"]
        C1 --> C2["Summary statistics paths"]
        C2 --> C3["Raw summary statistics\n(Parquet · partitioned by studyId)"]
    end

    %% --- STEP 3: Harmonisation ---
    subgraph S3["③ FinngenUkbMvpMetaSumstatHarmonisationStep"]
        G1["VariantIndex"] --> G2["VariantDirection"]
        C3 --> D1["Allele flipping & filtering"]
        B1 --> D1
        G2 --> D1
        D1 --> E1["Harmonised summary statistics\n(Parquet · partitioned by studyId)"]
    end

    %% --- STEP 4: QC ---
    subgraph S4["④ FinngenUkbMvpMetaStudyIndexQCAnnotationStep"]
        E1 --> Q1["SummaryStatisticsQC"]
        Q1 --> Q2["StudyIndex annotated with QC"]
        C1 --> Q2
    end

    %% --- STYLING ---
    classDef input fill:#f8f8ff,stroke:#555,stroke-width:1px,color:#000;
    classDef output fill:#e7ffe7,stroke:#555,stroke-width:1px,color:#000;

    class A1,A2,A3,A4,A5 input;
    class C3,E1,Q1,Q2 output;
```

??? tip "Inputs" - [x] `source_manifest_path`: manifest with summary statistics file paths and study metadata. - [x] `efo_curation_path`: EFO curation file for disease mapping. - [x] `gnomad_variant_index_path`: gnomAD variant index used for allele-flip direction. - [x] `finngen_release`: FinnGen release identifier used to filter EFO mappings (default `"R12"`).

??? tip "Outputs" - [x] Raw summary statistics in Parquet format, partitioned by `studyId`. - [x] Harmonised summary statistics in Parquet format, partitioned by `studyId`. - [x] Summary statistics QC results in Parquet format. - [x] Study index in Parquet format (updated with QC flags).

## Steps API

::: gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaSummaryStatisticsIngestionStep

### Individual steps

::: gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaStudyIndexStep

::: gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaSumstatConversionStep

::: gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaSumstatHarmonisationStep

::: gentropy.finngen_ukb_mvp_meta.FinngenUkbMvpMetaStudyIndexQCAnnotationStep
