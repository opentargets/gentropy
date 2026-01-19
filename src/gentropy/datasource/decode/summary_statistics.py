"""deCODE summary statistics datasource module."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session


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
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        session: Session,
        n_threads: int = 10,
    ) -> None:
        """Convert txt.gz (tsv) summary statistics to Parquet format.

        This method reads multiple gzipped TSV summary statistics files,
        processes them in parallel using the specified number of threads,
        and writes the combined output in Parquet format, partitioned by studyId.

        Args:
            summary_statistics_list (list[str]): List of summary statistics paths.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics in Parquet format.
            session (Session): Gentropy session.
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
                        f.lit("deCODE-proteomics-smp"),
                        f.regexp_extract(
                            f.input_file_name(), r"^.*/(Proteomics_.*)\.txt.gz$", 1
                        ),
                    ),
                )
                .repartitionByRange(15, "Chrom", "POS")
                .write.mode("append")
                .partitionBy("studyId")
                .parquet(output_path)
            )

        session.logger.info(
            f"Converting gzipped summary statistics to Parquet at {raw_summary_statistics_output_path}."
        )
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            pool.map(
                lambda path: process_one(path, raw_summary_statistics_output_path),
                summary_statistics_list,
            )
