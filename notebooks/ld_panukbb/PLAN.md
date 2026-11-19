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

The other three populations are only on public S3 (`s3://pan-ukb-us-east-1/ld_release/`, readable
anonymously over HTTPS). Each has `UKBB.{POP}.ldadj.bm` and `UKBB.{POP}.ldadj.variant.b38.ht`.
Sizes are estimated from block counts at EUR's ~119 MiB/block:

| Pop | Variants (BM rows) | Stored blocks | ≈ size  |
| --- | ------------------ | ------------- | ------- |
| AMR | 11,713,690         | 31,903        | 3.6 TiB |
| EAS | 10,225,280         | 24,892        | 2.8 TiB |
| MID | 14,127,512         | 45,296        | 5.2 TiB |

- **Only AFR, CSA, EUR are mirrored on GCS.** (MR `PLAN.md` item 2 says Pan-UKB is AWS-only;
  that is out of date for these three.) Dataproc clusters can read AWS, and the LD index is a
  one-off build, so all six populations are read from S3 directly (decision 3; the GCS copies
  are byte-identical). Production reads the GCS copy: gentropy's `LDMatrixInterface` /
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
  ~10–15× more I/O** than the current build: ~44 TiB for all six populations (EUR/AFR/CSA 32.5 TiB + AMR/EAS/MID ~11.6 TiB),
  vs ~3 TiB.
  FinnGen adds 186 GiB of text.
- The aligned index (`alleleOrder` = −1 where Pan-UKBB ref/alt is flipped vs the OT variant index)
  was built ad hoc in Oct 2024 by `PanUKBBLDMatrix.align_ld_index_alleles`. There is no step for
  it: `PanUKBBConfig._target_` points to `gentropy.pan_ukb_ingestion.PanUKBBVariantIndexStep`,
  which does not exist, and the config is not registered with Hydra. Only 2,093 EUR rows are
  flipped. It contains a handful of `*_alt` contigs (e.g. `22_KI270879v1_alt`) that need dropping.
- Panel sizes (null r² ≈ 1/N, the expected r² between unlinked variants):

  | Ancestry       | Pan-UKBB             | gnomAD v2 genomes  | Other                            |
  | -------------- | -------------------- | ------------------ | -------------------------------- |
  | European       | EUR 420,531 (2.4e-6) | NFE 7,718 (1.3e-4) | 1000G EUR 503 (2.0e-3)           |
  | Finnish        | (within EUR)         | FIN 1,738 (5.8e-4) | **FinnGen R12 520,210 (1.9e-6)** |
  | Ashkenazi      | (within EUR/MID)     | ASJ 145 (6.9e-3)   | —                                |
  | African        | AFR 6,636 (1.5e-4)   | AFR 4,359 (2.3e-4) | 1000G AFR 661                    |
  | East Asian     | EAS 2,709 (3.7e-4)   | EAS 780 (1.3e-3)   | 1000G EAS 504                    |
  | Admixed Amer.  | AMR 980 (1.0e-3)     | AMR 424 (2.4e-3)   | 1000G AMR 347                    |
  | South Asian    | CSA 8,876 (1.1e-4)   | —                  | 1000G SAS 489                    |
  | Middle Eastern | MID 1,599 (6.3e-4)   | —                  | —                                |

  Pan-UKBB is larger than gnomAD v2 for every population it has.

- **FinnGen R12 LD** (`gs://finngen-public-data-r12/ld_matrix/`, 186 GiB): long bgzipped/tabix
  TSV per chromosome (1–22, X), columns `#chr pos variant1 variant2 r r2`. It holds all pairs
  within **3 Mb** with r² > 0.01, in both directions, from PLINK 2 `--r-unphased`. IDs are
  already GRCh38 (`chr22_10557967_C_T`), so OT IDs come from dropping `chr`, with no liftover.
  r is rounded to 4 dp. The 3 Mb window (vs 10 Mb in Pan-UKBB) does not matter at r² ≥ 0.5.
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
   - **Size of the problem (26.06 GCST, `scripts/eas.py`).** Of 148,491 GCST studies, 22,726 have
     `eas` as the major population. In 4,587 of them, East Asian is not the main ancestry inside the
     `eas` bucket: 3,373 are South Asian, 799 Central Asian, 383 Middle Eastern and 32 Oceanian.
     All of them get gnomAD v2 East Asian LD (n ~0.8k) in clumping and PICS. Because sample sizes
     are summed per _mapped_ label, pooling also makes some multi-ancestry studies `eas`-major
     that would otherwise be European. Remapping South/Central Asian → `csa` and Middle Eastern →
     `mid` changes the major population of **6,055** studies: eas→csa 4,049, eas→nfe 1,669,
     eas→mid 330, eas→afr/amr 7. That includes **972 MR exposures** (1,498 are `eas`-major today).
     SuSiE's `eas → csa` hack then gives the ~18k genuinely East Asian studies South Asian LD;
     the South Asian ones get CSA only by accident.
   - Add `csa` and `mid` to `LD_Population`; remap South/Central Asian → `csa` and Middle
     Eastern → `mid` in the GWAS map. Keep `nfe`, `afr`, `eas` and `amr` as they are.
   - One shared map, study label → LD source (decision 3). Used by `LDAnnotator`,
     `LDMatrixInterface`, SuSiE and LDSC. This removes the `eas → csa` hack.
   - Store the index's `rValues.population` as the **study labels** (`nfe`, `fin`, `csa`…), so
     `LDAnnotator`'s join on the major population stays a plain equality. The source of each
     label is fixed by the map below and recorded in the step config and docs.
   - FIN usage, for scale: FinnGen R12 studies (2,336, all `fin`-major) come in as FinnGen's own
     SuSiE credible sets (`finngen_gwas_ingestion`: `finngen_studies` +
     `finngen_finemapping_ingestion`) and never touch the LD index. No GCST study in 26.06 has a
     `fin` population. The current consumer is the FinnGen R12 + MVP + UKBB meta DAG
     (`finngen_ukb_mvp_meta`: window clumping → `ld_based_clumping` → PICS): **13 of 330** studies
     in run 20261102T181409 are `fin`-major. Small, but FinnGen's panel is the best available
     and cheap to ingest.
3. **Sources (decided 2026-11-19): Pan-UKBB everywhere, except FinnGen for Finnish and gnomAD
   for Ashkenazi.**

   | Study label                         | Label status | LD source         | Location                                       |
   | ----------------------------------- | ------------ | ----------------- | ---------------------------------------------- |
   | `nfe` (+ `est`, `nwe`, `seu`)       | existing     | Pan-UKBB EUR      | S3                                             |
   | `afr`                               | existing     | Pan-UKBB AFR      | S3                                             |
   | `eas`                               | existing     | Pan-UKBB EAS      | S3                                             |
   | `amr`                               | existing     | Pan-UKBB AMR      | S3                                             |
   | `csa` (South/Central Asian)         | **new**      | Pan-UKBB CSA      | S3                                             |
   | `mid` (Middle Eastern)              | **new**      | Pan-UKBB MID      | S3                                             |
   | `fin`                               | existing     | FinnGen R12       | `gs://finngen-public-data-r12/ld_matrix`       |
   | `asj`                               | existing     | gnomAD v2.1.1 ASJ | `gs://gcp-public-data--gnomad` (existing code) |

   - **All six Pan-UKBB populations are read from S3** (`s3a://pan-ukb-us-east-1/ld_release/`),
     with one path template: `UKBB.{POP}.ldadj.bm` and `UKBB.{POP}.ldadj.variant.b38.ht`. The
     GCS mirror of EUR/AFR/CSA is **byte-identical** to S3: same `metadata.json` hashes, same
     part names and sizes (checked 2026-11-19), so reading S3 changes no results. It gives one
     code path, one canonical source, and no dependency on the 2024 mirror. The cost is reading
     ~44 TiB cross-cloud instead of ~11.6 TiB. Measure throughput in the chr22 benchmark. The
     path stays configurable, so the GCS mirror is a drop-in fallback if S3 throughput is the
     bottleneck.
   - Dataproc reads S3 with the anonymous credentials provider; gentropy already ships
     `hadoop-aws` via `external/s3`. Pan-UKB is in the AWS Open Data registry, so egress is
     probably covered. Confirm before the full run.
   - This covers the index only. SuSiE reads per locus at run time and stays on the GCS mirror,
     where the data are the same. Giving it EAS/AMR/MID would mean copying them to
     `gs://panukbb-ld-matrixes` (separate decision).
   - **ASJ — query before building.** No study in 26.06 has `asj` (or `est`/`nwe`/`seu`) in its
     `ldPopulationStructure`. The GWAS map has no Ashkenazi label, and the current index does not
     even include asj (`LDIndexConfig.ld_populations` = afr, amr, eas, fin, nfe). N = 145
     (null r² 6.9e-3) is also too noisy to be useful. Suggest dropping `asj` (and the unused
     `est`/`nwe`/`seu` labels, or mapping them to EUR) unless a datasource needs it. If kept, it
     reuses `GnomADLDMatrix` for that one population (~free).
   - Oceanian / Aboriginal Australian (32 `eas`-major studies) have no panel anywhere and stay on
     `eas`.

4. **Allele alignment** must become a real step, rerun against the current variant index. It
   should drop non-canonical contigs and apply `alleleOrder` to the sign of r. Sign only matters for the
   stored r, not r², but the index stores r.
5. **Threshold:** keep min r² 0.5 for the index. The extra reach (10 Mb band) only adds pairs that
   really pass 0.5. MR's r² < 0.001 needs are served separately (below), not by lowering the
   index threshold: at 0.001 the index would be enormous.

## Implementation steps (gentropy)

1. **Alignment step.** New `PanUKBBLDIndexAlignmentStep` (or fix `PanUKBBConfig._target_`):
   `align_ld_index_alleles` per pop against the current variant index, filter to chr 1–22, X.
   Run it for all six pops from the S3 `UKBB.{POP}.ldadj.variant.b38.ht`.
   Register the config. Add tests on a tiny Hail table fixture.
2. **`PanUKBBLDMatrix.as_ld_index(min_r2)`**, mirroring `GnomADLDMatrix.as_ld_index`:
   `BlockMatrix.read(...).entries(keyed=False)` → filter `|r| ≥ √min_r2` → join `i`/`j` to the
   aligned index → apply `alleleOrder_i × alleleOrder_j` → require same chromosome → symmetrise
   (the matrix is upper-triangular, like gnomAD, so reuse `_transpose_ld_matrix`) → aggregate
   with `_aggregate_ld_index_across_populations` (move it to a shared helper). Emit the
   unaggregated `(variantIdI, variantIdJ, r, chromosome, population)` frame per source, so the
   three sources can be unioned before a single aggregation.
   2b. **`FinnGenLDMatrix` (new, `datasource/finngen/ld.py`)**: read
   `finngen_r12_chr*_ld.tsv.gz` with Spark CSV (tab, header `#chr`). Keep `r2 ≥ min_r2`, strip
   `chr` from both IDs, and label `population = "fin"`. Files already hold both directions, so
   **do not** transpose. Add the diagonal (self, r = 1) for every variant that appears, to match
   the other sources. Check that `r`'s sign is relative to the alt allele of the ID as written
   (PLINK 2 `--r-unphased` uses the alt dosage). Re-key any FinnGen IDs that differ from the OT
   variant index by allele order, as for Pan-UKBB. bgzip is not splittable by default, so chr2
   (16 GiB) is one task. That is acceptable for a one-off; otherwise use a BGZF-aware reader.
   2c. **gnomAD ASJ** (only if kept, decision 3): `GnomADLDMatrix(ld_populations=["asj"])`, with
   the cross-chromosome fix (step 7).
3. **Step/config:** a new `LDIndexStep` composition, e.g. `ld_index` with per-source config
   blocks (`pan_ukbb: {populations, bm_path, aligned_index_path}`, `finngen: {path}`,
   `gnomad: {populations}`), which unions the per-source frames and aggregates once. Keep the
   gnomAD-only path available for comparison runs. Cluster config needs the S3A anonymous
   credentials provider for `s3a://pan-ukb-us-east-1`. Update `docs/python_api/`.
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
     Also gnomAD fin vs FinnGen R12, and gnomAD eas vs Pan-UKBB EAS.
   - Sign check: for pairs present in both FinnGen and Pan-UKBB EUR, `sign(r)` should mostly
     agree. A systematic flip means an allele-order bug.
   - Index size (pairs per variant) and pair-distance distribution vs gnomAD.
   - Downstream rerun of `ld_based_clumping` / PICS on the GWAS Catalog sumstats loci. Count the
     changes in `UNRESOLVED_LD` ("Variant not found in LD reference", 387k loci today) and in the
     no-population flag (140k), the number of loci clumped as LD-explained, and the credible-set
     size distribution.
7. **Fix the gnomAD cross-chromosome tag bug** while there (same-chromosome filter in
   `_resolve_variant_indices`).
8. Orchestration: new output path (e.g. `gs://…/ld_index/pan_ukbb_v1`). The index path is
   passed explicitly as `{gnomad}/v2.1.1/ld_index` in `gwas_catalog_sumstats_pics`,
   `gwas_catalog_top_hits`, `decode_ingestion` (twice), `decode_smp_finemapping` and
   `finngen_ukb_mvp_meta`. Repoint those and run the release comparison. Studies need the
   regenerated study index (new `csa`/`mid` labels) before the new index is useful.

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

- Confirm S3 reads from Dataproc carry no egress charge (AWS Open Data) and that `s3a://`
  BlockMatrix reads in Hail work with anonymous credentials. Try a chr22 slice of EAS first.
- ASJ: keep via gnomAD or drop (no study uses it today)?
- Copy EAS/AMR/MID to GCS as well, so SuSiE can use them (and the `eas → csa` hack can go for
  fine-mapping too)?
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
- `eas.py`: GCST studies affected by mapping South/Central Asian and Middle Eastern to `eas`
  (26.06 study table), and the major-population changes under a `csa`/`mid` remap.
