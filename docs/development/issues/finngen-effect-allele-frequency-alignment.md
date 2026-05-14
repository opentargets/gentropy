# PRD: Align FinnGen Effect Allele Frequency to gnomAD Variant Index

## Problem Statement

The `effectAlleleFrequencyFromSource` field is populated differently — and incorrectly — across the three FinnGen ingestion pipelines:

| Source                             | Column used                                                                | Problem                                                                                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| FinnGen SuSiE finemapping          | `maf`                                                                      | MAF is always ≤ 0.5; when the effect allele (allele2) has AF > 0.5, `maf = 1 − af(allele2)` and the stored value is wrong. Finnish-specific. |
| FinnGen standard summary stats     | `af_alt`                                                                   | Correct semantics (effect allele = alt), but Finnish-specific population frequency, not a reference population.                              |
| FinnGen + UKBB + MVP meta-analysis | Weighted cohort average, direction-corrected via gnomAD `VariantDirection` | Consistent with gnomAD reference; already correct.                                                                                           |

Two distinct problems are conflated:

1. **Semantic bug** (finemapping only): `maf` is the minor allele frequency, not the effect allele frequency. For common variants where AF(alt) > 0.5, the value is systematically `1 − AF(alt)` instead of `AF(alt)`.
2. **Population source inconsistency** (finemapping + standard summary stats): Finnish-population AFs from FinnGen differ from the global or Finnish reference frequencies in gnomAD, creating inconsistency across OpenTargets datasets that are aligned to gnomAD.

## Solution

Replace the FinnGen-source allele frequency with an allele frequency derived from the gnomAD variant index, with appropriate direction correction relative to the effect allele. This brings FinnGen finemapping and summary statistics in line with how the FinnGen + UKBB + MVP meta-analysis pipeline already works, and aligns the field semantics with gnomAD-based datasets (e.g., eQTL Catalogue summary stats, GWAS Catalog).

The gnomAD variant index carries an `alleleFrequencies` array with per-population AFs. For FinnGen (a Finnish biobank), the most relevant population is Finnish (`fin_adj`). If Finnish AF is unavailable for a variant, a fallback to global AF (`adj`) should be applied.

Direction correction is required: the gnomAD `alleleFrequencies` are always reported for the ALT allele relative to the gnomAD reference. If FinnGen's effect allele (allele2) is the REF in gnomAD (i.e., the variant is flipped), the AF must be transformed as `1 − gnomAD_AF`.

## User Stories

1. As a FinnGen finemapping user, I want `effectAlleleFrequencyFromSource` to represent the allele frequency of the effect allele (allele2), not the minor allele frequency, so that the field has correct semantics for all variants regardless of allele frequency direction.
2. As a FinnGen summary statistics user, I want `effectAlleleFrequencyFromSource` to be sourced from gnomAD rather than from the Finnish-specific `af_alt` column, so that the field is consistent with gnomAD-aligned sources across the platform.
3. As a FinnGen finemapping user, I want the gnomAD allele frequency to be drawn from the Finnish population (`fin_adj`) where available, with a fallback to global adjusted AF, so that the most relevant reference population is used for FinnGen data.
4. As a FinnGen finemapping user, I want the gnomAD AF to be direction-corrected (i.e., `1 − AF` when the effect allele is flipped relative to gnomAD REF), so that the frequency always corresponds to the allele whose `beta` was estimated.
5. As a data consumer, I want all FinnGen sources to populate `effectAlleleFrequencyFromSource` from the same reference (gnomAD), so that frequency-based filters and enrichment analyses behave consistently across FinnGen finemapping, summary stats, and meta-analysis datasets.
6. As a pipeline operator, I want the FinnGen SuSiE finemapping step to accept a gnomAD variant index path as an optional input, so that the allele frequency join can be performed without restructuring the entire ingestion graph.
7. As a data consumer, I want `effectAlleleFrequencyFromSource` to be null for FinnGen variants that are absent from the gnomAD variant index, rather than falling back to the incorrect FinnGen `maf` value, so that missing data is explicit rather than silently wrong.

## Implementation Decisions

### Root Cause per Pipeline

**FinnGen SuSiE finemapping** (`datasource/finngen/finemapping.py`, line 321):

```python
# BEFORE (wrong): maf ≤ 0.5, does not track effect allele direction
f.col("maf").cast("float").alias("effectAlleleFrequencyFromSource"),
```

The `maf` field is defined in `raw_finemapping_schema` as a string; it is the Finnish MAF from the SuSiE SNP file. For variants where AF(allele2) > 0.5, the stored value is `1 − AF(allele2)`.

**FinnGen standard summary statistics** (`datasource/finngen/summary_stats.py`, line 81):

```python
# BEFORE (consistent semantics, wrong population): Finnish af_alt
f.col("af_alt").cast("float").alias("effectAlleleFrequencyFromSource"),
```

`af_alt` is already the ALT allele frequency, so the semantic bug does not apply here. The problem is population specificity.

### Fix Approach

Both FinnGen finemapping and standard summary statistics ingestion steps should join with the gnomAD variant index and extract a reference-population allele frequency for the effect allele. The join key is `variantId`.

Population priority for FinnGen:

1. `fin_adj` (gnomAD Finnish adjusted) — most relevant
2. `adj` (gnomAD global adjusted) — fallback if Finnish is absent

Direction correction: The gnomAD `alleleFrequencies` are keyed to the gnomAD ALT allele. Determine whether the FinnGen effect allele matches gnomAD ALT by comparing the FinnGen `variantId` (chr_pos_ref_alt in FinnGen convention) to the gnomAD `variantId`. If they match, use gnomAD AF directly; if the variant is a flip (FinnGen ref/alt are swapped relative to gnomAD), apply `1 − gnomAD_AF`.

The `VariantDirection` dataset, already used by the meta-analysis step, encodes exactly this flip logic and is the right abstraction to reuse.

### Modules to Modify

1. **FinnGen SuSiE finemapping step** (`datasource/finngen/finemapping.py`) — after reading raw SNP data, join with gnomAD variant index; extract Finnish AF (or global fallback); apply direction correction; replace `maf` with corrected gnomAD AF.
2. **FinnGen standard summary statistics** (`datasource/finngen/summary_stats.py`) — same join and extraction logic; replace `af_alt` with corrected gnomAD AF.
3. **FinnGen ingestion step config** (`config.py` or the relevant step classes) — add `gnomad_variant_index_path` as an optional parameter to the FinnGen SuSiE and summary statistics steps (already present in the meta-analysis step).
4. **`VariantDirection`** or a new utility function — extract and expose a helper that returns `effectAlleleFrequency(variantId, populationPriority, variantDirection)` to avoid duplicating the population-lookup + direction-flip logic across three places.

### What Does Not Change

- FinnGen + UKBB + MVP meta-analysis (`finngen_meta/summary_statistics.py`): already correct; no changes needed.
- The `effectAlleleFrequencyFromSource` field name, type (`FloatType`, nullable), and position in the schema.
- FinnGen `studyId` construction or any other columns.

## Testing Decisions

### What Makes a Good Test

Tests should verify output values for known synthetic inputs, not internal join logic. A good test provides a small FinnGen input with known `maf`/`af_alt` values and a matching gnomAD variant index row, and asserts the resulting `effectAlleleFrequencyFromSource` equals the expected gnomAD AF (possibly direction-corrected).

### Modules to Test

- **FinnGen finemapping** — unit test with:
  - A variant where AF(alt) < 0.5: assert gnomAD `fin_adj` AF is used, not `maf`.
  - A variant where AF(alt) > 0.5 and the variant is flipped: assert `1 − gnomAD_AF` is stored.
  - A variant absent from gnomAD: assert `null` is stored.
- **FinnGen summary statistics** — same three cases applied to `af_alt` replacement.
- **End-to-end consistency check**: for variants present in both FinnGen finemapping and FinnGen summary statistics, assert that `effectAlleleFrequencyFromSource` values match (within float tolerance) after the fix.

### Prior Art

FinnGen meta-analysis allele flip tests in `tests/gentropy/datasource/finngen_meta/`.
`VariantDirection` tests in `tests/gentropy/dataset/test_variant_direction.py`.

## Out of Scope

- Changing `effectAlleleFrequencyFromSource` in non-FinnGen sources (eQTL Catalogue, GWAS Catalog, SuSiE-inf, UKB-PPP).
- Choosing a different reference population for non-Finnish studies (a separate generalisation task).
- Backfilling historical FinnGen parquet outputs — this fix applies to future ingestion runs.
- Adding a per-population AF breakdown to the `StudyLocus` or `SummaryStatistics` schema.

## Further Notes

- The FinnGen SuSiE `maf` column is explicitly documented in FinnGen's format description as the minor allele frequency in the Finnish sample. It is not the effect allele frequency, and the bug is not conditional on any edge case — it affects every variant where the effect allele is also the major allele.
- The FinnGen + UKBB + MVP meta-analysis step already takes `gnomad_variant_index_path` as an input and uses `VariantDirection.from_variant_index(variant_index)`. Reusing the same pattern for the other two FinnGen steps minimises new code.
- gnomAD variant IDs use the convention `chr_pos_ref_alt` where `ref`/`alt` match the gnomAD GRCh38 reference. FinnGen uses the same convention. A flip is detectable by string comparison alone (no additional genomic data needed).
