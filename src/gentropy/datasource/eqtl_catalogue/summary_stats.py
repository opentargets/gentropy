"""Summary statistics ingestion for eQTL Catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column


@dataclass
class EqtlCatalogueSummaryStats:
    """Summary statistics dataset for eQTL Catalogue."""

    @staticmethod
    def _full_study_id_regexp() -> Column:
        """Constructs a full study ID from the URI.

        Returns:
            Column: expression to extract a full study ID from the URI.
        """
        # Example of a URI which is used for parsing:
        # "gs://genetics_etl_python_playground/input/preprocess/eqtl_catalogue/imported/GTEx_V8/ge/Adipose_Subcutaneous.tsv.gz".

        # Regular expession to extract project ID from URI.  Example: "GTEx_V8".
        _project_id = f.regexp_extract(
            f.input_file_name(),
            r"imported/([^/]+)/.*",
            1,
        )
        # Regular expression to extract QTL group from URI.  Example: "Adipose_Subcutaneous".
        _qtl_group = f.regexp_extract(f.input_file_name(), r"([^/]+)\.tsv\.gz", 1)
        # Extracting gene ID from the column.  Example: "ENSG00000225630".
        _gene_id = f.col("gene_id")

        # We can now construct the full study ID based on all fields.
        # Example: "GTEx_V8_Adipose_Subcutaneous_ENSG00000225630".
        return f.concat(_project_id, f.lit("_"), _qtl_group, f.lit("_"), _gene_id)

    @classmethod
    def from_source(
        cls: type[EqtlCatalogueSummaryStats],
        summary_stats_df: DataFrame,
    ) -> SummaryStatistics:
        """Ingests all summary stats for all eQTL Catalogue studies.

        Args:
            summary_stats_df (DataFrame): an ingested but unprocessed summary statistics dataframe from eQTL Catalogue.

        Returns:
            SummaryStatistics: a processed summary statistics dataframe for eQTL Catalogue.
        """
        processed_summary_stats_df = (
            summary_stats_df.select(
                # Construct study ID from the appropriate columns.
                cls._full_study_id_regexp().alias("studyId"),
                # Add variant information.
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("ref"),
                    f.col("alt"),
                ).alias("variantId"),
                f.col("chromosome"),
                f.col("position").cast(t.IntegerType()),
                # Parse p-value into mantissa and exponent.
                *split_pvalue_column(f.col("pvalue")),
                # Add beta, standard error, and allele frequency information.
                f.col("beta").cast("double"),
                f.col("se").cast("double").alias("standardError"),
                f.col("maf").cast("float").alias("effectAlleleFrequencyFromSource"),
            )
            # Drop rows which don't have proper position or beta value.
            .filter(
                f.col("position").cast(t.IntegerType()).isNotNull()
                & (f.col("beta") != 0)
            )
        )

        # Initialise a summary statistics object.
        return SummaryStatistics(
            _df=processed_summary_stats_df,
            _schema=SummaryStatistics.get_schema(),
        )
