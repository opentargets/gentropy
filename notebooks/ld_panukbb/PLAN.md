# Pan-UKBB LD index — plan

Tracks [opentargets/issues#4693](https://github.com/opentargets/issues/issues/4693) ("Update LD
index generation to use PanUKBB instead of GnomAD"). Side project to the MR work
(`notebooks/mendelian_randomisation/PLAN.md`, action items 1 and 2), but the change is to gentropy
itself and affects every consumer of `LDIndex`.

Investigation date: 2026-11-19. Numbers below come from `scripts/` (see the end of this file).

## Where we are today

| Piece                                  | Code                                                                                                                          | LD source                                                |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| **LD index** (`LDIndex`, r² ≥ 0.5)     | `gnomad_ingestion.LDIndexStep` → `datasource/gnomad/ld.py::GnomADLDMatrix.as_ld_index`                                        | gnomAD v2.1.1 (GRCh37, lifted over); afr/amr/eas/fin/nfe |
| Consumers of the index                 | `StudyLocus.annotate_ld` → `method/ld.py::LDAnnotator` (PICS, `ld_based_clumping`, `credible_set_qc` clump mode, `susie_inf`) | the index above                                          |
| Per-locus LD matrices for fine-mapping | `method/ld_matrix_interface.py` → `datasource/pan_ukbb_ld/ld.py::PanUKBBLDMatrix` (nfe/csa/afr), gnomAD otherwise             | **already Pan-UKBB**                                     |

So Pan-UKBB is already read in production (SuSiE), via `gs://panukbb-ld-matrixes`. Only the
LD _index_ still uses gnomAD. Current output: `gs://gnomad_data_2/ld_index/v2.1.1` (51 GiB,
partitioned by chromosome, schema `assets/schemas/ld_index.json`).

## What is in `gs://panukbb-ld-matrixes`

| Pop | Variants (BM rows) | BM size  | Aligned GRCh38 index                         |
| --- | ------------------ | -------- | -------------------------------------------- |
| EUR | 23,960,350         | 14.1 TiB | `UKBB.EUR.aligned.parquet` (23,905,433 rows) |
| AFR | 22,082,831         | 12.0 TiB | `UKBB.AFR.aligned.parquet`                   |
| CSA | 16,161,130         | 6.4 TiB  | `UKBB.CSA.aligned.parquet`                   |

Plus the GRCh38 lifted Hail variant tables under `ukb-diverse-pops-public-build-38/`.

- **Only AFR, CSA, EUR are mirrored.** Pan-UKBB also publishes AMR, EAS and MID (AWS,
  `pan-ukb-us-east-1/ld_release`). (MR `PLAN.md` item 2 says Pan-UKB is AWS-only; that is out of
  date for these three.) Production reads the GCS copy: gentropy's `LDMatrixInterface` /
  SuSiE defaults point at `gs://panukbb-ld-matrixes`, orchestration (checked
  `~/pipeline/orchestration`) never overrides `ld_matrix_paths` and has no S3 support
  (`utils/path.py`: "s3:// (currently not implemented)"), and the copy was made under
  opentargets/issues#3455 ("Migrate pan-UKBB LD matrixes from AWS to GCP", closed 2024-10-01).
  This is not yet confirmed from a job log.
- Block matrices are **upper-triangular and banded**: blockSize 4096, 0.36% of blocks stored, band
  median 21 blocks (max 37) in EUR. 21 × 4096 variants spans a median **10.1 Mb** (p10 8.8,
  p90 12.2). Pairs up to ~10 Mb apart are stored. This is the same radius as TwoSampleMR's
  `clump_kb = 10000`. The gnomAD index's p99 pair distance is 533 kb.
- For scale: gnomAD v2.1.1 nfe is 14.2M variants / 555 GiB, afr 1.6 TiB. **Scanning Pan-UKBB is
  ~10× more I/O** than the current build (32.5 TiB vs ~3 TiB).
- The aligned index (`alleleOrder` = −1 where Pan-UKBB ref/alt is flipped vs the OT variant index)
  was built ad hoc in Oct 2024 by `PanUKBBLDMatrix.align_ld_index_alleles`. There is no step for
  it: `PanUKBBConfig._target_` points to `gentropy.pan_ukb_ingestion.PanUKBBVariantIndexStep`,
  which does not exist, and the config is not registered with Hydra. Only 2,093 EUR rows are
  flipped. It contains a handful of `*_alt` contigs (e.g. `22_KI270879v1_alt`) that need dropping.
- Sample sizes (approx., to confirm from the Pan-UKBB docs): Pan-UKBB EUR ~420k, CSA ~8.9k,
  AFR ~6.6k, EAS ~2.7k, MID ~1.6k, AMR ~1k, against gnomAD v2 genomes nfe ~7.7k, afr ~4.4k,
  fin ~1.7k, eas ~0.8k, amr ~0.4k. r² noise between unlinked variants is ~1/n.
- Matrices are `ldadj`: as I understand it, r is computed on covariate-adjusted genotypes.
  **Check** this is acceptable for PICS/clumping. It should be (it is what SuSiE already uses).

## Comparison on chr22 (current index vs Pan-UKBB EUR)

| Metric                           | gnomAD index                        | Pan-UKBB EUR |
| -------------------------------- | ----------------------------------- | ------------ |
| Variants in reference            | 433,598 any pop; 193,900 with nfe r | 297,352      |
| MR IVs covered (4,178 chr22 IVs) | 95.7% any pop; **87.0% nfe**        | **94.2%**    |
| IVs only in this reference       | 70 (nfe)                            | 370          |

**Existing bug:** the gnomAD index has 145 chr22 pairs (28 lead variants) whose tag is on
**chrX**, an artefact of liftover. `_resolve_variant_indices` takes the chromosome from `i` only and never
checks `j`. The Pan-UKBB build must require `chromosome(i) == chromosome(j)`. It is worth
patching the gnomAD path too.

## Design decisions

1. **Keep the `LDIndex` schema unchanged** (`variantId, chromosome, ldSet[tagVariantId,
rValues[population, r]]`). Then `LDAnnotator`, PICS and clumping do not change. Only the
   population labels change.
2. **Population vocabulary.** The study index speaks gnomAD (`LD_Population`: afr, amr, asj, eas,
   est, fin, nfe, nwe, seu). `assets/data/gwas_population_2_LD_panel_map.json` maps
   **South Asian, Central Asian and Greater Middle Eastern → `eas`**, because gnomAD v2 had no
   SAS/MID LD. SuSiE also hard-codes `eas → csa` as a "temporary solution"
   (`susie_finemapper.py`), and `LDMatrixInterface.ancestry_map` and `ldsc._infer_ld_ancestry`
   each hold their own map. Proposal:
   - Add `csa` and `mid` to `LD_Population`; remap South/Central Asian → `csa` and Middle
     Eastern → `mid` in the GWAS map. Keep `nfe`, `afr`, `eas` and `amr` as they are.
   - One shared map, study label → Pan-UKBB panel: nfe/fin/asj/est/nwe/seu → EUR, afr → AFR,
     csa → CSA, eas → EAS, amr → AMR, mid → MID. Used by `LDAnnotator`, `LDMatrixInterface`,
     SuSiE and LDSC. This removes the `eas → csa` hack.
   - **Decide:** store index populations as Pan-UKBB labels (`EUR`…) and translate in
     `LDAnnotator`, or as study labels. Recommend Pan-UKBB labels plus translation: it is
     honest about the source, and FIN is then explicitly "EUR panel".
   - **FIN is lost** (no Finnish panel), but little depends on it. FinnGen R12 studies (2,336, all
     `fin`-major) are ingested as FinnGen's own SuSiE credible sets (`finngen_gwas_ingestion` DAG:
     `finngen_studies` + `finngen_finemapping_ingestion`), so they never touch the LD index. No
     GCST study in 26.06 has a `fin` population. The **only** consumer is the FinnGen R12 + MVP +
     UKBB meta DAG (`finngen_ukb_mvp_meta`: window clumping → `ld_based_clumping` → PICS): in run
     20261102T181409, **13 of 330** studies are `fin`-major (317 `nfe`). Those 13 would move to
     the EUR panel. Check them by comparing gnomAD fin and Pan-UKBB EUR r for shared pairs.
3. **Populations to build.** Phase 1: EUR, AFR, CSA (already on GCS). Phase 2: copy EAS, AMR, MID
   from AWS. EAS ~2.7k still beats gnomAD's ~0.8k, but **check AWS egress cost** (several TiB).
   Until then, EAS/AMR studies either fall back to gnomAD eas/amr or are flagged as unresolved
   LD (to decide). A **hybrid index** (Pan-UKBB pops plus gnomAD eas/amr/fin) is the lowest-risk
   transition and fits the existing `rValues[population]` schema.
4. **Allele alignment** must become a real step, rerun against the current variant index. It
   should drop non-canonical contigs and apply `alleleOrder` to the sign of r. Sign only matters for the
   stored r, not r², but the index stores r.
5. **Threshold:** keep min r² 0.5 for the index. The extra reach (10 Mb band) only adds pairs that
   really pass 0.5. MR's r² < 0.001 needs are served separately (below), not by lowering the
   index threshold: at 0.001 the index would be enormous.

## Implementation steps (gentropy)

1. **Alignment step.** New `PanUKBBLDIndexAlignmentStep` (or fix `PanUKBBConfig._target_`):
   `align_ld_index_alleles` per pop against the current variant index, filter to chr 1–22, X.
   Register the config. Add tests on a tiny Hail table fixture.
2. **`PanUKBBLDMatrix.as_ld_index(min_r2)`**, mirroring `GnomADLDMatrix.as_ld_index`:
   `BlockMatrix.read(...).entries(keyed=False)` → filter `|r| ≥ √min_r2` → join `i`/`j` to the
   aligned index → apply `alleleOrder_i × alleleOrder_j` → require same chromosome → symmetrise
   (the matrix is upper-triangular, like gnomAD, so reuse `_transpose_ld_matrix`) → aggregate
   with `_aggregate_ld_index_across_populations` (move it to a shared helper).
3. **Step/config:** extend `LDIndexConfig` with `ld_source: "gnomad" | "pan_ukbb"`, or add
   `PanUKBBLDIndexConfig` + `cs.store(name="pan_ukbb_ld_index")`. Prefer a separate step: the
   inputs differ completely. Update `docs/python_api/`.
4. **Population map refactor** (decision 2). Update `LD_Population`, the GWAS map, and
   `LDAnnotator._get_major_population` / `_calculate_r2_major` for translation. Remove the SuSiE
   `eas → csa` hack. Update `test_ld.py`, `test_study_index.py` and the doctests.
5. **Benchmark on chr22 first** (~300k EUR variants, ~1.5k blocks ≈ 190 GiB). Measure Hail
   `entries()` throughput, extrapolate to 32.5 TiB, and size the cluster. EUR has ~2×10¹²
   entries in stored blocks. If `entries()` is too slow, process blocks directly
   (e.g. `BlockMatrix.filter` per chromosome window → `to_numpy` → `np.nonzero`, parallelised
   with Spark) instead of materialising every entry as a row.
6. **Validate** (chr22, then genome-wide):
   - r(gnomAD nfe) vs r(Pan-UKBB EUR) for shared pairs. Expect agreement, with Pan-UKBB tighter.
     Also fin vs EUR.
   - Index size (pairs per variant) and pair-distance distribution vs gnomAD.
   - Downstream rerun of `ld_based_clumping` / PICS on the GWAS Catalog sumstats loci. Count the
     changes in `UNRESOLVED_LD` ("Variant not found in LD reference", 387k loci today) and in the
     no-population flag (140k), the number of loci clumped as LD-explained, and the credible-set
     size distribution.
7. **Fix the gnomAD cross-chromosome tag bug** while there (same-chromosome filter in
   `_resolve_variant_indices`).
8. Orchestration: new output path (e.g. `gs://…/ld_index/pan_ukbb_v1`), point the PICS/clumping
   configs at it, and run the release comparison.

## MR thread: our IV clumping vs TwoSampleMR

How each method selects instruments:

| Distance from a stronger IV (same exposure) | Ours (upstream)                        | TwoSampleMR `clump_data` default               |
| ------------------------------------------- | -------------------------------------- | ---------------------------------------------- |
| < 500 kb                                    | always removed (window)                | removed only if r² ≥ 0.001                     |
| 500 kb – 10 Mb                              | removed only if r² ≥ 0.5 (gnomAD)      | removed if r² ≥ 0.001                          |
| Variant not in LD reference                 | **kept** (never LD-checked)            | **dropped** (ieugwasr/plink drop missing SNPs) |
| Reference                                   | gnomAD v2 nfe ~7.7k, by major ancestry | 1000G EUR n = 503, all studies                 |

What our data shows:

- **Distance-only bound (not a TwoSampleMR run, no LD used):** `scripts/window.py` re-clumps
  the existing v2 exposure table (861,343 IV rows, 19,847 exposures, median 6 IVs) per exposure
  and chromosome, greedily by |β/SE|, dropping any IV within w bp of a stronger kept IV. It keeps
  **100%** at 0.5 Mb (confirming the upstream contract), **85.7%** at 1 Mb and **50.6%** at
  10 Mb. 50.6% is what TwoSampleMR would keep _if_ every same-exposure pair within 10 Mb looked
  linked, i.e. the "it is just a 10 Mb window" hypothesis taken literally.
- With n = 503, the r² of two unlinked variants is ~χ²₁/503 and exceeds 0.001 about 48% of the
  time. Against our set (which has no pairs < 500 kb, so clumping can only remove), a weaker IV
  with k stronger neighbours within 10 Mb survives by chance with probability ~0.52^k. So the true
  1000G retention from LD lies between 50.6% and 100%, nearer 50.6% for dense exposures. Variants
  absent from 1000G are dropped on top of that, which can push it below 50.6%. With Pan-UKBB EUR (n ~420k, noise ~2×10⁻⁶), r² < 0.001 becomes a
  real independence threshold.
- Only **677,919 distinct same-exposure IV pairs** lie within 10 Mb (73k within 1 Mb), across
  235,068 distinct IV variants. The r for all of them can be pulled from the Pan-UKBB block
  matrices: they fall inside the 10 Mb band. The output is tiny, but the IVs touch almost every
  block, so it costs one full matrix pass. Do it **in the same pass** as the LD-index build
  (emit `|r| ≥ 0.707` for the index, plus all entries where both i and j are IVs), or on chr22
  first.

Proposed comparison (four instrument sets, then rerun the MR sweep and validation panel on each):

| Arm | Rule                                                            | How                                                                     |
| --- | --------------------------------------------------------------- | ----------------------------------------------------------------------- |
| A   | Current (500 kb window + r² ≥ 0.5 gnomAD)                       | existing `exposure_table`                                               |
| B   | TwoSampleMR default: r² < 0.001, 10 Mb, 1000G EUR, drop missing | plink `--clump` with GRCh38 1000G EUR (or `ieugwasr::ld_clump` locally) |
| C   | Same rule with Pan-UKBB EUR (ancestry-matched where available)  | IV-pair r from the matrix pass above + greedy clump in Spark            |
| D   | 10 Mb window                                                    | `scripts/window.py` logic                                               |

Read-outs: IVs kept per exposure, set overlap A/B/C/D, B vs D agreement (tests the "≈ window"
hypothesis), how many B drops are "not in 1000G", and the effect on λ, Cochran's Q and the
validation panel (LDL→CAD, BMI→T2D, SBP→CAD/stroke/hypertension, urate→gout, HDL→CAD,
CAD→LDL). Look at a few loci where B and C disagree (e.g. dense lipid loci such as
APOA1/APOA5, LPL) to see which rule removes real independent signals. Do this **before** fixing
the MR significance threshold (MR PLAN item 5), since IV count drives the SE.

Cheap first step: arm D vs A on the full table is already done (above). Arm B on chr22 only
needs a chr22 1000G EUR reference and plink. Arm C on chr22 needs the chr22 benchmark (step 5).

## Open questions

- AWS egress for EAS/AMR/MID: who pays, and is it worth it given small panels?
- Hybrid vs Pan-UKBB-only index for the first release?
- Does OT orchestration pin the LD index path (`gs://gnomad_data_2/ld_index/v2.1.1`) anywhere
  besides gentropy configs? Check the orchestration repo.
- Confirm the "ldadj" definition and the exact per-population N from the Pan-UKBB LD release
  notes.

## Scripts (`scripts/`)

Throwaway profiling, run locally with polars. Inputs are downloaded from GCS or come from the MR
local data dir (`/Users/dc16/data/mendelian_randomisation/inputs_v2_26.06`).

- `band.py`: band width / stored-block fraction from `UKBB.{POP}.ldadj/metadata.json`
  (download as `meta_{POP}.json`).
- `eur_index.py <dir>`: profile `UKBB.EUR.aligned.parquet` (in `<dir>/eur/`), plus the bp span
  of N blocks.
- `ldidx.py <dir>`, `cov.py <dir>`: profile the gnomAD index for chr22 (`<dir>/ldidx22/`), find
  cross-chromosome tags, and compare IV coverage with Pan-UKBB EUR.
- `iv.py`: same-exposure IV pair counts by distance.
- `window.py`: greedy window clumping of the exposure table by |z|.
