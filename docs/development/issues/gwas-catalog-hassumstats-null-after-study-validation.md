# Ensure GWAS Catalog top-hit studies have explicit `hasSumstats = false`

## Problem

After `StudyValidationStep`, some GWAS Catalog studies can have `hasSumstats = NULL`.

This is surprising because downstream consumers treat `hasSumstats` as a boolean availability flag. For GWAS Catalog studies that are available only from top-hit associations, the expected value is `false`, not null.

## Root Cause

`StudyValidationStep` does not derive `hasSumstats`; it reads study indices and preserves/merges the incoming flag.

The GWAS Catalog top-hit ingestion path writes studies through:

- `src/gentropy/gwas_catalog_top_hits.py`
- `StudyIndexGWASCatalog.add_no_sumstats_flag()` in `src/gentropy/datasource/gwas_catalog/study_index.py`

`add_no_sumstats_flag()` currently adds `SUMSTATS_NOT_AVAILABLE` to `qualityControls`, but it does not set `hasSumstats = false`.

During study validation, `StudyIndex.deconvolute_studies()` contains this behavior:

- If any row for a `studyId` has `hasSumstats = true`, output `true`.
- Else if any row for that `studyId` has `hasSumstats = false`, output `false`.
- Else leave `hasSumstats` as null.

So top-hit-only GWAS Catalog studies can remain null after validation when no source row provided an explicit false value.

## Scope

Implement the minimal semantic fix:

- Update `StudyIndexGWASCatalog.add_no_sumstats_flag()` so it sets `hasSumstats = false` in addition to adding the `SUMSTATS_NOT_AVAILABLE` QC flag.
- Keep the method name and call sites unchanged.
- Do not change `StudyValidationStep` behavior unless a regression test exposes a separate validation bug.

## Proposed Code Change

In `src/gentropy/datasource/gwas_catalog/study_index.py`, change `add_no_sumstats_flag()` from only updating `qualityControls` to also updating `hasSumstats`:

```python
self.df = (
    self.df
    .withColumn(
        "qualityControls",
        f.array(f.lit(StudyQualityCheck.SUMSTATS_NOT_AVAILABLE.value)),
    )
    .withColumn("hasSumstats", f.lit(False))
)
```

## Tests

Add or update focused tests:

- `tests/gentropy/datasource/gwas_catalog/test_gwas_catalog_study_index.py`

  - Assert `add_no_sumstats_flag()` adds `SUMSTATS_NOT_AVAILABLE`.
  - Assert `add_no_sumstats_flag()` sets `hasSumstats` to `false`.

- `tests/gentropy/dataset/test_study_index.py`
  - Add a regression case for `deconvolute_studies()` where a top-hit-only row with `hasSumstats = false` remains false after deconvolution.
  - Optionally add a case documenting the current null-preserving behavior when all duplicate rows have null `hasSumstats`, if we want to keep that behavior explicit.

## Acceptance Criteria

- GWAS Catalog top-hit-only studies emitted by `GWASCatalogTopHitIngestionStep` have `hasSumstats = false`.
- After `StudyValidationStep`, GWAS Catalog top-hit-only studies do not have `hasSumstats = NULL`.
- Existing sumstats-backed GWAS Catalog studies still have `hasSumstats = true` when annotated by `annotate_sumstats_qc()`.
- Tests cover the regression path.

## Out Of Scope

- Making `hasSumstats` non-nullable in `study_index.json`.
- Reworking `StudyIndex.deconvolute_studies()` merge semantics.
- Changing the study validation output split logic or default invalid QC reasons.
