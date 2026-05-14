# PRD: Unified Locus Effect-Size Schema

## Problem Statement

The `locus` array struct inside `StudyLocus` carries two fields — `beta` and `standardError` — that are intended to represent per-variant marginal summary statistics. In practice, their semantics differ across ingestion pipelines:

| Source                  | `locus.beta`                           | `locus.standardError`                          |
| ----------------------- | -------------------------------------- | ---------------------------------------------- |
| eQTL Catalogue          | Marginal regression coefficient ✓      | Marginal SE ✓                                  |
| FinnGen (SuSiE)         | **Posterior mean** (from `mean{i}`) ✗  | Marginal SE ✓ (inconsistent pair)              |
| SuSiE-inf (in-pipeline) | **Posterior mean** (from `mu`) ✗       | NULL (posterior precision `omega` discarded) ✗ |
| PICS (curated/sumstats) | Marginal beta (lead only), NULL (tags) | **PICS probability proxy** (not a real SE) ✗   |
| GWAS Catalog (curated)  | Derived from CI/p-value                | Derived from CI/p-value                        |

This inconsistency makes it impossible to:

- Correctly combine marginal statistics across sources (e.g., for colocalization, meta-analysis).
- Store SuSiE posterior quantities for downstream use (e.g., MR, posterior-weighted colocalization).
- Distinguish whether a `beta` value is a marginal effect or a posterior conditional effect.

## Solution

Extend the `locus` struct with two new nullable fields — `posteriorBeta` and `posteriorStandardError` — to hold SuSiE posterior quantities, while ensuring `beta` and `standardError` are strictly marginal (or explicitly null) across all sources. Remove the PICS `standardError` proxy, which is in −log₁₀(p) space and is not a valid standard error.

## User Stories

1. As a gentropy developer, I want `locus.beta` to always represent the marginal regression coefficient (or null if unavailable), so that I can rely on a consistent interpretation across all study-locus sources.
2. As a gentropy developer, I want `locus.standardError` to always represent the marginal standard error of beta (or null if unavailable), so that downstream consumers do not misinterpret PICS probability values as a standard error.
3. As a gentropy developer, I want `locus.posteriorBeta` to store the SuSiE posterior mean of the effect size for each variant in the credible set, so that posterior-weighted analyses (e.g., MR, SuSiE-inf colocalisation) have access to the correct posterior quantities.
4. As a gentropy developer, I want `locus.posteriorStandardError` to store the SuSiE posterior standard deviation of the effect size, so that uncertainty in the posterior effect is properly represented.
5. As a FinnGen ingestion user, I want the FinnGen locus struct to correctly populate `beta` and `standardError` from the marginal GWAS columns (`beta`, `se`) rather than from the SuSiE posterior columns (`mean{i}`, `sd{i}`), so that marginal statistics are not silently replaced by posterior ones.
6. As a FinnGen ingestion user, I want `posteriorBeta` and `posteriorStandardError` to be populated from the SuSiE `mean{i}` and `sd{i}` columns per credible set index, so that both marginal and posterior information are preserved.
7. As a SuSiE-inf user, I want the in-pipeline fine-mapping step to carry marginal `beta` and `standardError` into the locus struct from the input summary statistics, so that marginal statistics are not lost after fine-mapping.
8. As a SuSiE-inf user, I want `posteriorBeta` to be populated from the SuSiE-inf `mu` matrix output (posterior means), so that the posterior effect is accessible.
9. As a SuSiE-inf user, I want `posteriorStandardError` to be derived from the SuSiE-inf `omega` matrix output (posterior precisions, where SD = 1/√ω), so that posterior uncertainty is not silently discarded.
10. As a PICS user, I want the PICS locus struct to not include a `standardError` field populated from the PICS internal probability proxy, so that consumers are not misled by values in p-value space (0–1) that look like standard errors.
11. As a PICS user, I want `locus.beta` for the lead variant to remain populated from the marginal summary statistics, so that at least the index variant retains its effect size.
12. As an eQTL Catalogue ingestion user, I want `posteriorBeta` and `posteriorStandardError` to be explicitly null in the locus struct, since eQTL Catalogue FTP files do not export SuSiE `mu` or `omega`, so that the schema is uniform.
13. As a GWAS Catalog top-hit ingestion user, I want `posteriorBeta` and `posteriorStandardError` to be explicitly null, since curated associations have no fine-mapping posterior quantities.
14. As a `StudyLocus` schema maintainer, I want the `get_locus_schema()` method (or equivalent) to include `posteriorBeta` (DoubleType, nullable) and `posteriorStandardError` (DoubleType, nullable), so that schema validation passes uniformly across all sources.
15. As a `StudyLocus` annotation user, I want any method that appends rows to the `locus` array (e.g., `annotate_locus_statistics`) to fill the two new fields with nulls where they are not computed, so that the schema remains consistent after annotation.
16. As a downstream analyst, I want a documented mapping in the code of which fields are populated per source, so that I understand provenance at a glance.

## Implementation Decisions

### Schema Changes

Add two new nullable `DoubleType` fields to the `locus` array struct element:

- **`posteriorBeta`** — SuSiE posterior mean of the effect size conditional on the variant being causal (from `mu` in SuSiE-inf, from `mean{i}` in FinnGen SuSiE). Null for PICS, eQTL Catalogue, GWAS Catalog.
- **`posteriorStandardError`** — SuSiE posterior standard deviation of effect, derived as `1/√ω` from precision `omega` in SuSiE-inf, or directly from `sd{i}` in FinnGen SuSiE. Null for all sources that do not provide it.

The existing fields `beta` and `standardError` retain their positions in the struct and their nullable semantics — only their population changes per-source.

### Modules to Modify

1. **`StudyLocus` locus schema definition** — add `posteriorBeta` and `posteriorStandardError` fields.
2. **FinnGen SuSiE finemapping** (`datasource/finngen/finemapping.py`) — fix `beta`/`standardError` to use marginal columns; populate `posteriorBeta`/`posteriorStandardError` from `mean{i}`/`sd{i}` per credible set index.
3. **SuSiE-inf pipeline** (`susie_finemapper.py`) — carry marginal `beta`/`standardError` from input dataframe into locus struct; extract `omega` from SuSiE-inf output; populate `posteriorBeta` from `mu`, `posteriorStandardError` from `1/√ω`.
4. **PICS method** (`method/pics.py`) — remove the `standardError` field from the PICS locus struct (`PICSED_LOCUS_SCHEMA`) and from the `finemap()` method. Remove `pics_snp_std` computation and `tag_dict["standardError"]` assignment.
5. **eQTL Catalogue finemapping** (`datasource/eqtl_catalogue/finemapping.py`) — add explicit null literals for `posteriorBeta` and `posteriorStandardError` in locus struct assembly.
6. **GWAS Catalog associations** (`datasource/gwas_catalog/associations.py`) — add explicit null literals for both new fields.
7. **Any other `StudyLocus` construction sites** — add null literals for both fields to maintain schema consistency (e.g., `annotate_locus_statistics`, `from_parquet` downstream join methods).

### Source-by-Source Population Matrix

| Source         | `beta`                              | `standardError`             | `posteriorBeta`                 | `posteriorStandardError`              |
| -------------- | ----------------------------------- | --------------------------- | ------------------------------- | ------------------------------------- |
| eQTL Catalogue | Marginal (from `credible_sets.tsv`) | Marginal SE                 | NULL                            | NULL                                  |
| FinnGen SuSiE  | Marginal (from `beta` column)       | Marginal SE (from `se`)     | `mean{credibleSetIndex}`        | `sd{credibleSetIndex}`                |
| SuSiE-inf      | Marginal (from input SS)            | Marginal SE (from input SS) | `mu[variant, credibleSetIndex]` | `1/√omega[variant, credibleSetIndex]` |
| PICS           | Lead: marginal; Tags: NULL          | NULL (field removed)        | NULL                            | NULL                                  |
| GWAS Catalog   | Derived (CI/p+beta)                 | Derived (CI/p+beta)         | NULL                            | NULL                                  |

### Pre-existing Bugs to Fix

- **FinnGen** (`finemapping.py`): `mean{i}` is aliased to `beta_{i}` and then used as `beta` in the locus struct — raw marginal `beta` column is never selected. Fix: select `f.col("beta")` as marginal; select `mean{i}` and `sd{i}` for posteriors.
- **SuSiE-inf** (`susie_finemapper.py` lines 992–993): column spelled `"StandardError"` (capital S) causes marginal SE not to be carried through. Fix: lowercase the column reference.
- **SuSiE-inf** (`susie_finemapper_from_prepared_dataframes`): `df_columns = ["variantId", "z"]` drops `beta` and `standardError` before they reach the fine-mapping call. Fix: preserve `beta` and `standardError` (or `z`) in the input dataframe passed to `susie_inf_to_studylocus`.
- **SuSiE-inf**: `omega` from `susie_inf()` return dict is extracted but immediately discarded. Fix: pass `omega` through to the locus struct assembly alongside `mu`.

### Commit Sequence (Safe Ordering)

1. **Schema change**: Add `posteriorBeta` and `posteriorStandardError` to `StudyLocus` locus struct schema. All construction sites fill both fields with null literals — no behavioral change.
2. **PICS cleanup**: Remove `standardError` from `PICSED_LOCUS_SCHEMA` and from the `finemap()` computation. Add null literals for both new fields.
3. **eQTL Catalogue**: Add null literals for both new fields. No other changes.
4. **FinnGen SuSiE fix**: Fix marginal `beta`/`standardError`; populate `posteriorBeta`/`posteriorStandardError` from `mean{i}`/`sd{i}`.
5. **SuSiE-inf fix**: Fix `"StandardError"` typo; fix `df_columns` to preserve marginal stats; extract `omega`; populate all four locus fields.
6. **GWAS Catalog / remaining sources**: Add null literals for both new fields.
7. **Integration test update**: Update schema snapshot tests, schema validation tests, and any test fixtures that reference the locus struct.

## Testing Decisions

### What Makes a Good Test

- Tests should verify **external behavior** (output schema, output values, data contracts) rather than implementation details (internal variable names, intermediate DataFrames).
- Schema tests should assert that a produced `StudyLocus` passes `StudyLocus.validate_schema()`.
- Value tests should assert specific column values for known synthetic inputs, especially for the FinnGen and SuSiE-inf fixes where prior values were silently wrong.

### Modules to Test

- **`StudyLocus` schema**: Verify `posteriorBeta` and `posteriorStandardError` exist as nullable DoubleType fields in the locus element schema.
- **FinnGen finemapping**: Unit test with a synthetic input row containing both marginal (`beta`, `se`) and posterior (`mean1`, `sd1`) columns. Assert `locus.beta == marginal_beta`, `locus.posteriorBeta == mean1`.
- **PICS method**: Assert that the output locus struct does not contain a `standardError` field, and that `beta` is null for non-lead variants.
- **SuSiE-inf**: Integration test asserting that `posteriorBeta` is populated from `mu` and `posteriorStandardError` from `1/√omega` for at least one synthetic credible set.
- **eQTL Catalogue finemapping**: Assert that `posteriorBeta` and `posteriorStandardError` are null for all locus entries.

### Prior Art

Existing locus schema tests: `tests/gentropy/dataset/test_study_locus.py`.
Existing PICS tests: `tests/gentropy/method/test_pics.py`.
Existing FinnGen ingestion tests: `tests/gentropy/datasource/finngen/`.

## Out of Scope

- Recovering signed marginal beta for PICS non-lead tag variants. Direction cannot be recovered without signed LD (`r`, not `r²`), which PICS does not store. MAF and sample size can reconstruct SE magnitude but not beta sign.
- Exporting SuSiE `mu`/`omega` for eQTL Catalogue. These are not available in the eQTL Catalogue FTP format (`credible_sets.tsv` + `lbf_variable.txt`).
- Changes to the `SummaryStatistics` dataset schema.
- Changes to the `StudyIndex` schema.
- Downstream colocalization or MR pipeline changes to consume the new fields.
- Refactoring `normalise_gwas_statistics` in `common/stats.py`.

## Further Notes

- The PICS `standardError` field stores `10^(-pics_snp_std)`, where `pics_snp_std` is in −log₁₀(p) units derived from LD (`r²`) and the lead p-value. This results in values in the range (0, 1] that are easily confused with real standard errors but are dimensionally incorrect. No downstream method should consume them.
- SuSiE-inf `omega` (posterior precision) has shape `(p, L)` matching `mu`. `posteriorStandardError = 1/√omega` yields units consistent with `posteriorBeta`.
- The FinnGen source files contain `mean1`–`mean10` and `sd1`–`sd10` (one column per credible set index). The correct posterior pair for credible set `k` is `mean{k}` and `sd{k}`.
- After the FinnGen fix, `locus.beta` and `locus.posteriorBeta` will both be non-null for FinnGen variants in credible sets. They will generally differ, because the posterior mean shrinks the effect toward zero.
