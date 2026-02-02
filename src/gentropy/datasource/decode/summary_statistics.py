"""deCODE summary statistics datasource module."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, SummaryStatistics
from gentropy.common.processing import normalize_chromosome
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.decode import deCODEDataSource


class deCODESummaryStatistics:
    """deCODE summary statistics class."""

    N_THREAD_OPTIMAL = 10
    N_THREAD_MAX = 500

    raw_schema = t.StructType(
        [
            t.StructField("Chrom", t.StringType()),
            t.StructField("Pos", t.LongType()),
            t.StructField("Name", t.StringType()),
            t.StructField("rsids", t.StringType()),
            t.StructField("effectAllele", t.StringType()),
            t.StructField("otherAllele", t.StringType()),
            t.StructField("Beta", t.DoubleType()),
            t.StructField("Pval", t.DoubleType()),
            t.StructField("minus_log10_pval", t.DoubleType()),
            t.StructField("SE", t.DoubleType()),
            t.StructField("N", t.LongType()),
            t.StructField("impMAF", t.DoubleType()),
        ]
    )

    @classmethod
    def txtgz_to_parquet(
        cls,
        session: Session,
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        n_threads: int = 500,  # across all pyspark workers
    ) -> None:
        """Convert txt.gz (tsv) summary statistics to Parquet format.

        This method reads multiple gzipped TSV summary statistics files,
        processes them in parallel using the specified number of threads,
        and writes the combined output in Parquet format, partitioned by studyId.

        Args:
            session (Session): Gentropy session.
            summary_statistics_list (list[str]): List of summary statistics paths.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics in Parquet format.
            n_threads (int): Number of threads to use.

        """
        if len(summary_statistics_list) == 0:
            session.logger.warning("No summary statistics paths found to process.")
            return

        if not isinstance(n_threads, int) or n_threads < 1:
            session.logger.warning(
                f"Invalid n_threads value: {n_threads}. Falling back to 10 threads."
            )
            n_threads = cls.N_THREAD_OPTIMAL
        if n_threads < cls.N_THREAD_OPTIMAL:
            session.logger.warning(
                f"Using low n_threads value: {n_threads}. This may lead to sub-optimal performance."
            )
        if n_threads > cls.N_THREAD_MAX:
            session.logger.warning(
                f"Using high n_threads value: {n_threads}, this may lead to overloading spark driver. Limiting to 32."
            )
            n_threads = cls.N_THREAD_MAX

        def process_one(input_path: str, output_path: str) -> None:
            session.logger.info(
                f"Converting gzipped summary statistics to Parquet from {input_path} to {output_path}."
            )
            project_id = f.when(
                f.input_file_name().contains("SMP"),
                f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
            ).otherwise(deCODEDataSource.DECODE_PROTEOMICS_RAW.value)
            (
                session.spark.read.csv(
                    input_path,
                    sep="\t",
                    header=True,
                    schema=t.StructType(
                        [
                            t.StructField("Chrom", t.StringType()),
                            t.StructField("Pos", t.LongType()),
                            t.StructField("Name", t.StringType()),
                            t.StructField("rsids", t.StringType()),
                            t.StructField("effectAllele", t.StringType()),
                            t.StructField("otherAllele", t.StringType()),
                            t.StructField("Beta", t.DoubleType()),
                            t.StructField("Pval", t.DoubleType()),
                            t.StructField("minus_log10_pval", t.DoubleType()),
                            t.StructField("SE", t.DoubleType()),
                            t.StructField("N", t.LongType()),
                            t.StructField("impMAF", t.DoubleType()),
                        ]
                    ),
                )
                .withColumn(
                    "studyId",
                    f.concat_ws(
                        "_",
                        project_id,
                        f.regexp_extract(
                            f.input_file_name(), r"^.*/(Proteomics_.*)\.txt.gz$", 1
                        ),
                    ),
                )
                # Ensure that the size of each partition is ~100Mb
                .repartitionByRange(15, "Chrom", "POS")
                .write.mode("append")
                .partitionBy("studyId")
                .parquet(output_path)
            )

        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            pool.map(
                lambda path: process_one(path, raw_summary_statistics_output_path),
                summary_statistics_list[:2],
            )

    @classmethod
    def from_source(
        cls,
        raw_summary_statistics: DataFrame,
        variant_direction: VariantDirection,
    ) -> SummaryStatistics:
        """Create deCODESummaryStatistics from raw summary statistics DataFrame.

        Args:
            raw_summary_statistics (DataFrame): Raw summary statistics DataFrame.
            variant_direction (VariantDirection): VariantDirection Dataset.

        Returns:
            SummaryStatistics: deCODESummaryStatistics object.

        """
        vd = (
            # Need only positive strand variants
            variant_direction.df.filter(f.col("strand") == 1)
            .select(
                f.col("chromosome"),
                f.col("rangeId"),
                f.col("originalVariantId"),
                f.col("variantId"),
                f.col("direction"),
                f.col("isStrandAmbiguous"),
            )
            # NOTE: repartition("chromosome") produces very uneven partitions,
            # Spark attempts then to fall back to `dynamic partitioning` algorithm
            # which fails after N failures.
            .repartitionByRange(4_000, "chromosome", "variantId")
            .persist()
        ).alias("vd")

        pval = pvalue_from_neglogpval(f.col("minus_log10_pval"))
        _sumstats = _sumstats.select(
            f.col("studyId"),
            normalize_chromosome(f.col("Chrom")).alias("chromosome"),
            f.col("Pos").alias("position"),
            f.col("minus_log10_pval"),
            pval.mantissa.alias("pValueMantissa"),
            pval.exponent.alias("pValueExponent"),
        ).withColumn(
            "variantId",
            f.concat_ws(
                "_",
                f.col("chromosome"),
                f.col("position"),
                f.col("otherAllele"),
                f.col("effectAllele"),
            ),
        )

        _sumstats = _sumstats.select(
            f.col("studyId"),
            f.col("variantId"),
            f.col("chromosome"),
            f.col("position"),
            f.col("beta"),
            f.col("sampleSize"),
            f.col("pValueMantissa"),
            f.col("pValueExponent"),
            f.col("effectAlleleFrequencyFromSource"),
            f.col("standardError"),
        )

        return SummaryStatistics(_sumstats)
