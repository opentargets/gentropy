"""BigBrain summary statistics module.

This module provides:

- **`BigBrainSummaryStatistics`** – a utility class with two main pipelines:

  1. `download_tsv_gz` – streams a gzipped TSV file from a public HTTPS URL
     (Zenodo) to a Spark-readable path.
  2. `from_source` – harmonisation (schema alignment, effect-allele resolution
     from the source-provided `Allele` column, p-value parsing, and sanity
     filtering).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import requests
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import normalize_chromosome
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.bigbrain import BigBrainPublicationMetadata

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session

# Columns common to both the eQTL and sQTL "full_assoc"/"top_assoc" files.
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
        t.StructField("Allele", t.StringType()),
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
    ) -> SummaryStatistics:
        """Harmonise raw BigBrain full-association summary statistics.

        The harmonisation pipeline performs the following steps in order:

        1. **Effect-allele resolution** – the source `Allele` column names which of
           `ref`/`alt` the beta is relative to (it is not always `alt`), so the
           effect/other allele pair is read directly per row rather than assumed.
           No external reference join is needed, unlike deCODE's gnomAD-based flip.
        2. **Schema alignment** – renames BigBrain-specific column names, builds the
           source-oriented variant ID as `chromosome_position_otherAllele_effectAllele`,
           and constructs `studyId` as `BigBrain_{qtlType}_EUR_{feature}`.
        3. **Untested-row filtering** – drops `fixed_beta == 0` filler rows (mirrors
           `EqtlCatalogueSummaryStats.from_source`, which applies the same filter for
           the same reason).
        4. **P-value parsing** – `Fixed_P` (the fixed-effect meta-analysis p-value,
           consistent with `fixed_beta`/`fixed_sd`) is split into mantissa/exponent.
        5. **Sample size / EAF** – `sampleSize` is backfilled with the constant
           `BigBrainPublicationMetadata.SAMPLE_SIZE`; `effectAlleleFrequencyFromSource`
           is left null (not reported at the variant level by the source).
        6. **Sanity filter** – applies the standard `SummaryStatistics.sanity_filter`.

        Args:
            raw_summary_statistics (DataFrame): Raw `full_assoc` DataFrame as produced
                by reading the downloaded TSV with `FULL_ASSOC_SCHEMA`.
            qtl_type (str): QTL type identifier embedded in the study ID, e.g.
                `BigBrainQtlType.EQTL.value` or `BigBrainQtlType.SQTL.value`.

        Returns:
            SummaryStatistics: Harmonised summary statistics.
        """
        effect_allele = f.col("Allele")
        other_allele = f.when(
            f.col("ref") == effect_allele, f.col("alt")
        ).otherwise(f.col("ref"))

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
            .select(
                f.col("studyId"),
                f.col("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.lit(BigBrainPublicationMetadata().SAMPLE_SIZE)
                .cast(t.IntegerType())
                .alias("sampleSize"),
                *split_pvalue_column(f.col("pValue")),
                f.lit(None).cast(t.FloatType()).alias("effectAlleleFrequencyFromSource"),
                f.col("standardError"),
            )
        )

        return SummaryStatistics(
            _df=processed, _schema=SummaryStatistics.get_schema()
        ).sanity_filter()
