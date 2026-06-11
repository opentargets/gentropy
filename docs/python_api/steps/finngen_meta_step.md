---
title: FinnGen Meta-Analysis Ingestion
---

## Pipeline overview

The steps are independently runnable so study-index generation, conversion,
harmonisation, and QC can be scheduled separately. The two-way and three-way
analyses share study-index and QC steps but use separate harmonisers.

::: gentropy.finngen_meta.FinngenMetaStudyIndexStep

::: gentropy.finngen_meta.TwoWayMetaSumstatConversionStep

::: gentropy.finngen_meta.ThreeWayMetaSumstatConversionStep

::: gentropy.finngen_meta.TwoWayMetaSumstatHarmonisationStep

::: gentropy.finngen_meta.ThreeWayMetaSumstatHarmonisationStep

::: gentropy.finngen_meta.FinngenMetaStudyIndexQCAnnotationStep
