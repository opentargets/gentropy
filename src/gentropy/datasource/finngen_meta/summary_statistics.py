"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.common.session import Session

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.finngen_meta import MetaAnalysisDataSource


class FinnGenMetaSummaryStatistics:
    """FinnGen meta summary statistics ingestion and harmonisation."""

    @staticmethod
    def extract_study_phenotype_from_path(file_path: Column) -> Column:
        """Extract the study phenotype from finngen file path.

        Note:
            Assumes the file name format is some_path/to/<studyPhenotype>_meta_out.tsv.gz
        """
        return f.regexp_replace(
            f.element_at(f.split(file_path, "/"), -1), "_meta_out.tsv.gz", ""
        )

    @classmethod
    def bgzip_to_parquet(
        cls,
        session: Session,
        summary_statistics_glob: str,
        datasource: MetaAnalysisDataSource,
        raw_summary_statistics_output_path: str,
    ) -> None:
        """Convert gzipped summary statistics to Parquet format.

        Args:
            session (Session): Session object.
            summary_statistics_glob (str): Base path where summary statistics files are located.
            datasource (MetaAnalysisDataSource): Data source information.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
        """
        session.logger.info(
            f"Converting gzipped summary statistics from {summary_statistics_glob} to Parquet at {raw_summary_statistics_output_path}."
        )
        if not session.use_enhanced_bgzip_codec:
            session.logger.error(
                "The use_enhanced_bgzip_codec is set to False. This will lead to inefficient reading of block gzipped files."
            )
            raise KeyError(
                "Please set `session.spark.use_enhanced_bgzip_codec` to True in the Session configuration."
            )

        (
            session.spark.read.format("csv")
            .option("sep", "\t")
            .option("header", "true")
            .load(summary_statistics_glob)
            .withColumn(
                "studyId",
                f.concat_ws(
                    "_",
                    f.lit(datasource.value),
                    f.lit(cls.extract_study_phenotype_from_path(f.input_file_name())),
                ),
            )
            .write.mode(session.write_mode)
            .partitionBy("studyId")
            .parquet(raw_summary_statistics_output_path)
        )

    @classmethod
    def harmonise(
        cls,
        session: Session,
        raw_summary_statistics_path: str,
    ) -> SummaryStatistics:
        """Load raw summary statistics from Parquet files.

        Args:
            session (Session): Session object.
            raw_summary_statistics_path (str): Path to the raw summary statistics Parquet files.
            study_index (StudyIndex): Study index object.
        """
        raw_sumstats = session.spark.read.parquet(raw_summary_statistics_path)
        session.logger.info("Harmonising summary statistics.")

        sumstats = raw_sumstats.select(
            f.col("studyId"),
            f.col("variantId"),
            f.col("chromosome"),
            f.col("position").cast(t.IntegerType()).alias("position"),
            f.col("beta").cast(t.DoubleType()).alias("beta"),
            f.col("sampleSize").cast(t.IntegerType()).alias("sampleSize"),
            *pvalue_from_neglogpval(f.col("negLogPval")),
            f.lit(None).cast(t.DoubleType()).alias("effectAlleleFrequencyFromSource"),
            f.col("standardError").cast(t.DoubleType()).alias("standardError"),
        )
        return SummaryStatistics(_df=sumstats)
