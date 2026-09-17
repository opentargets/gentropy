"""BigBrain summary statistics module.

This module provides:

- **`BigBrainSummaryStatistics`** – a utility class with two main pipelines:

  1. `download_tsv_gz` – streams a gzipped TSV file from a public HTTPS URL
     (Zenodo) to a Spark-readable path.
  2. `from_source` – harmonisation (schema alignment, effect-allele resolution,
     gnomAD-based orientation correction, p-value parsing, and sanity filtering).

!!! note "`full_assoc` has no `Allele` column"
    The source README documents an `Allele` column (naming the effect allele) for
    both `full_assoc` and `top_assoc`, but the delivered `full_assoc.tsv.gz` files
    (verified for both the eQTL and sQTL EUR releases) do not actually contain it —
    only 16 columns are present, `ref`/`alt` immediately followed by `fixed_beta`.
    `top_assoc.tsv.gz`, which does retain `Allele`, was checked in full for both
    QTL types (97,739 rows total) and `Allele` equals `ref` in every single row,
    with zero exceptions. `ref` is therefore treated as the effect allele and `alt`
    as the other allele unconditionally, for both files.

!!! warning "`ref`/`alt` do not follow the genome-reference convention"
    Despite the naming, source `ref` is **not** reliably the GRCh38 reference-genome
    allele. An empirical check of a sample of harmonised variants against gnomAD's
    variant index (`VariantIndex`, joint EUR/`nfe_adj` frequencies) found that, among
    variants also present in gnomAD, source `ref` equals gnomAD's **alternate**
    allele ~99% of the time and gnomAD's **reference** allele only ~1% of the time —
    a near-total, systematic pattern, not noise. This is consistent with the
    resulting effect-allele frequency being skewed low (median ~0.21), as expected
    for an alt/minor-allele-as-effect-allele convention, not a true reference-allele
    convention.

    This does not make the harmonisation wrong: `variantId` is built as
    `chromosome_position_otherAllele_effectAllele` (`otherAllele` = source `alt`,
    `effectAllele` = source `ref`), so the ~99% case above already produces an ID
    in gnomAD's own `chromosome_position_referenceAllele_alternateAllele` order —
    the two inversions cancel. `from_source` still runs a gnomAD `VariantDirection`
    join to (a) correct the ~1% of variants where they don't cancel (flipping `beta`
    and normalising `variantId` to gnomAD's orientation) and (b) drop variants whose
    position is in gnomAD but whose alleles match neither orientation, so neither
    case silently produces a `variantId` that fails downstream gnomAD-keyed joins.
    Variants absent from gnomAD altogether are kept as-is (no evidence of a
    problem).

!!! note "Indels represented in both orientations in gnomAD"
    For some indels, gnomAD's own reference data lists the same event twice, in
    both orientations (e.g. both `1_100_TG_T` and `1_100_T_TG` as separate source
    variants) — confirmed against the full gnomAD v4.1 variant index, where this
    affects ~0.4% of matched sites. Left un-deduplicated, this makes a single
    BigBrain variantId match two `VariantDirection` entries with opposite
    directions, duplicating the row with contradictory corrections. `from_source`
    deduplicates the `VariantDirection` slice per `(chromosome, rangeId,
    variantId)`, keeping the exact/direct match over the flipped one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests
from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import normalize_chromosome
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE, VariantDirection
from gentropy.datasource.bigbrain import BigBrainPublicationMetadata

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session

# Columns of the "full_assoc" file (verified against the real Zenodo files for
# both eQTL and sQTL EUR releases). Note this does NOT include an `Allele`
# column, despite one being documented in the source README's data dictionary —
# see the module-level note above.
# `Fixed_P`/`Random_P` are read as strings because they include scientific
# notation (e.g. "2.80606e-174") that `split_pvalue_column` parses directly.
FULL_ASSOC_SCHEMA = t.StructType(
    [
        t.StructField("feature", t.StringType()),
        t.StructField("variant_id", t.StringType()),
        t.StructField("chr", t.StringType()),
        t.StructField("pos", t.LongType()),
        t.StructField("ref", t.StringType()),
        t.StructField("alt", t.StringType()),
        t.StructField("fixed_beta", t.DoubleType()),
        t.StructField("fixed_sd", t.DoubleType()),
        t.StructField("fixed_z", t.DoubleType()),
        t.StructField("Random_Z", t.DoubleType()),
        t.StructField("Fixed_P", t.StringType()),
        t.StructField("Random_P", t.StringType()),
        t.StructField("Fixed_bonf", t.DoubleType()),
        t.StructField("Random_bonf", t.DoubleType()),
        t.StructField("Fixed_FDR", t.DoubleType()),
        t.StructField("Random_FDR", t.DoubleType()),
    ]
)


@dataclass
class BigBrainSummaryStatistics:
    """Utility class for ingesting and harmonising BigBrain brain-tissue molecular QTL summary statistics.

    This class is never instantiated directly. It exposes two class-method pipelines:

    * `download_tsv_gz` – streams a gzipped TSV file from a public HTTPS Zenodo
      URL to a destination path (local or `gs://`), without holding the whole
      file in driver memory.

    * `from_source` – takes the raw ingested `full_assoc` DataFrame and produces
      a harmonised `SummaryStatistics` dataset.

    !!! note "Large downloads"
        The BigBrain EUR `full_assoc` files are 8-24GB gzipped each. `download_tsv_gz`
        streams in fixed-size chunks to bound memory usage, but downloading and
        parsing the full genome-wide file is still a multi-hour, high-disk operation
        best run on dedicated infrastructure rather than a local/dev environment.
    """

    CHUNK_SIZE = 1 << 20
    """Streaming download chunk size in bytes (1 MiB)."""

    @classmethod
    def download_tsv_gz(cls, url: str, output_path: str, session: Session) -> None:
        """Stream a gzipped TSV file from a public HTTPS URL to `output_path`.

        Args:
            url (str): Public HTTPS URL of the gzipped TSV file (e.g. a Zenodo file URL).
            output_path (str): Destination path for the downloaded file. May be a local
                path or a `gs://` URI (both are opened transparently via `fsspec`).
            session (Session): Gentropy session, used only for logging.
        """
        import fsspec

        session.logger.info(f"Downloading {url} to {output_path}.")
        with requests.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with fsspec.open(output_path, "wb") as out_file:
                for chunk in response.iter_content(chunk_size=cls.CHUNK_SIZE):
                    out_file.write(chunk)

    @classmethod
    def from_source(
        cls,
        raw_summary_statistics: DataFrame,
        qtl_type: str,
        variant_direction: VariantDirection,
        flipping_window_size: int = DEFAULT_WINDOW_SIZE,
    ) -> SummaryStatistics:
        """Harmonise raw BigBrain full-association summary statistics.

        The harmonisation pipeline performs the following steps in order:

        1. **Effect-allele resolution** – `full_assoc` carries no per-row effect-allele
           indicator, but `top_assoc`'s `Allele` column (which does name the effect
           allele) equals `ref` in every one of the 97,739 real rows checked across
           both QTL types, with zero exceptions, so `ref` is treated as the effect
           allele and `alt` as the other allele unconditionally.
        2. **Schema alignment** – renames BigBrain-specific column names, builds the
           source-oriented variant ID as `chromosome_position_otherAllele_effectAllele`,
           and constructs `studyId` as `BigBrain_{qtlType}_EUR_{feature}`.
        3. **Untested-row filtering** – drops `fixed_beta == 0` filler rows (mirrors
           `EqtlCatalogueSummaryStats.from_source`, which applies the same filter for
           the same reason).
        4. **gnomAD orientation correction** – despite the effect-allele resolution
           above, source `ref`/`alt` do not reliably follow the genome-reference
           convention (see the module-level warning). Variants are left-joined
           against the gnomAD `VariantDirection` reference (positive strand only) on
           `(chromosome, rangeId, variantId)`:
             - Variants that resolve to gnomAD's flipped orientation have `beta`
               negated and `variantId` normalised to gnomAD's `originalVariantId`.
             - Variants that resolve directly (the dominant case, ~99% of matched
               variants empirically) are left unchanged.
             - Variants whose position is in gnomAD but whose alleles match neither
               orientation are dropped — this is a confirmed allele mismatch, not
               just missing reference data.
             - Variants absent from gnomAD altogether are kept unchanged (no evidence
               the reported orientation is wrong).
           The `VariantDirection` reference is deduplicated per `(chromosome,
           rangeId, variantId)` first, keeping the exact/direct match — some indels
           are listed in both orientations in gnomAD itself, which would otherwise
           duplicate the row with contradictory corrections (see module docstring).
        5. **P-value parsing** – `Fixed_P` (the fixed-effect meta-analysis p-value,
           consistent with `fixed_beta`/`fixed_sd`) is split into mantissa/exponent.
        6. **Sample size / EAF** – `sampleSize` is backfilled with the constant
           `BigBrainPublicationMetadata.SAMPLE_SIZE`; `effectAlleleFrequencyFromSource`
           is left null (not reported at the variant level by the source).
        7. **Sanity filter** – applies the standard `SummaryStatistics.sanity_filter`.

        !!! note "Variant flipping window"
            `flipping_window_size` **must** match the window used when building the
            `VariantDirection` dataset. A mismatch will silently produce incorrect
            join keys.

        Args:
            raw_summary_statistics (DataFrame): Raw `full_assoc` DataFrame as produced
                by reading the downloaded TSV with `FULL_ASSOC_SCHEMA`.
            qtl_type (str): QTL type identifier embedded in the study ID, e.g.
                `BigBrainQtlType.EQTL.value` or `BigBrainQtlType.SQTL.value`.
            variant_direction (VariantDirection): gnomAD variant-direction reference
                used to correct variant orientation (see step 4 above).
            flipping_window_size (int): Genomic window size (bp) used to partition the
                `VariantDirection` dataset for the join. Must match the value used
                when building the `VariantDirection` dataset. Defaults to
                `DEFAULT_WINDOW_SIZE`.

        Returns:
            SummaryStatistics: Harmonised summary statistics.
        """
        effect_allele = f.col("ref")
        other_allele = f.col("alt")

        processed = (
            raw_summary_statistics.select(
                f.concat_ws(
                    "_",
                    f.lit("BigBrain"),
                    f.lit(qtl_type),
                    f.lit(BigBrainPublicationMetadata().ANCESTRY),
                    f.col("feature"),
                ).alias("studyId"),
                normalize_chromosome(f.col("chr")).alias("chromosome"),
                f.col("pos").cast(t.IntegerType()).alias("position"),
                other_allele.alias("otherAllele"),
                effect_allele.alias("effectAllele"),
                f.col("fixed_beta").cast(t.DoubleType()).alias("beta"),
                f.col("fixed_sd").cast(t.DoubleType()).alias("standardError"),
                f.col("Fixed_P").alias("pValue"),
            )
            .filter(f.col("beta") != 0)
            .withColumn(
                "variantId",
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("otherAllele"),
                    f.col("effectAllele"),
                ),
            )
            .withColumn(
                "rangeId",
                f.floor(f.col("position") / flipping_window_size).cast(t.IntegerType()),
            )
        )

        # Indel representation is sometimes ambiguous in gnomAD itself (e.g. both
        # "1_100_TG_T" and "1_100_T_TG" appear as separate source variants for the
        # same underlying event), so a single BigBrain variantId can match two
        # VariantDirection entries with opposite directions. Deduplicating on
        # (chromosome, rangeId, variantId) keeps the exact/direct match
        # (direction=1) over the flipped one, and prevents row-duplication with
        # contradictory corrections.
        exact_match_first = Window.partitionBy(
            "chromosome", "rangeId", "variantId"
        ).orderBy(f.col("direction").desc())
        vd_slice = (
            # BigBrain alleles are compared only with positive-strand reference entries,
            # mirroring deCODE's gnomAD-based flip.
            variant_direction.df.filter(f.col("strand") == 1)
            .select(
                f.col("chromosome"),
                f.col("rangeId"),
                f.col("variantId"),
                f.col("originalVariantId"),
                f.col("direction"),
            )
            .withColumn("_rank", f.row_number().over(exact_match_first))
            .filter(f.col("_rank") == 1)
            .drop("_rank")
            .persist()
        )
        # Positions covered by gnomAD, regardless of which specific alleles are
        # reported there. Used to tell "not in gnomAD" (kept as-is) apart from "in
        # gnomAD, but with different alleles" (dropped) for variants whose exact
        # variantId doesn't match the join above.
        gnomad_positions = (
            vd_slice.select(
                f.col("chromosome"),
                f.split(f.col("originalVariantId"), "_")
                .getItem(1)
                .cast(t.IntegerType())
                .alias("position"),
            )
            .distinct()
            .withColumn("_inGnomad", f.lit(True))
        )

        processed = (
            processed.join(
                vd_slice, on=["chromosome", "rangeId", "variantId"], how="left"
            )
            .join(gnomad_positions, on=["chromosome", "position"], how="left")
            .filter(f.col("direction").isNotNull() | f.col("_inGnomad").isNull())
            .select(
                f.col("studyId"),
                f.coalesce(f.col("originalVariantId"), f.col("variantId")).alias(
                    "variantId"
                ),
                f.col("chromosome"),
                f.col("position"),
                (f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))).alias(
                    "beta"
                ),
                f.lit(BigBrainPublicationMetadata().SAMPLE_SIZE)
                .cast(t.IntegerType())
                .alias("sampleSize"),
                *split_pvalue_column(f.col("pValue")),
                f.lit(None)
                .cast(t.FloatType())
                .alias("effectAlleleFrequencyFromSource"),
                f.col("standardError"),
            )
        )

        vd_slice.unpersist()

        return SummaryStatistics(
            _df=processed, _schema=SummaryStatistics.get_schema()
        ).sanity_filter()
