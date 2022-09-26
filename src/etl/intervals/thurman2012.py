from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.intervals.Liftover import LiftOverSpark


class ParseThurman:
    """
    Parser Thurman 2012 dataset

    :param Thurman_parquet: path to the parquet file containing the Thurman 2012 data
    :param gene_index: Pyspark dataframe containing the gene index
    :param lift: LiftOverSpark object

    **Summary of the logic:**

    - Lifting over coordinates to GRCh38
    - Mapping genes names to gene IDs -> we might need to measure the loss of genes if there are obsoleted names.
    """

    # Constants:
    DATASET_NAME = "thurman2012"
    DATA_TYPE = "interval"
    EXPERIMENT_TYPE = "dhscor"
    PMID = "22955617"
    BIO_FEATURE = "aggregate"

    def __init__(
        self: ParseThurman,
        etl: ETLSession,
        thurman_datafile: str,
        gene_index: DataFrame,
        lift: LiftOverSpark,
    ) -> None:

        self.etl = etl

        etl.logger.info("Parsing Thurman 2012 data...")
        etl.logger.info(f"Reading data from {thurman_datafile}")

        thurman_schema = t.StructType(
            [
                t.StructField("gene_chr", t.StringType(), False),
                t.StructField("gene_start", t.IntegerType(), False),
                t.StructField("gene_end", t.IntegerType(), False),
                t.StructField("gene_name", t.StringType(), False),
                t.StructField("chrom", t.StringType(), False),
                t.StructField("start", t.IntegerType(), False),
                t.StructField("end", t.IntegerType(), False),
                t.StructField("score", t.FloatType(), False),
            ]
        )

        # Process Thurman data in a single step:
        self.Thurman_intervals = (
            etl.spark
            # Read table according to the schema, then do some modifications:
            .read.csv(thurman_datafile, sep="\t", header=False, schema=thurman_schema)
            .select(
                f.regexp_replace(f.col("chrom"), "chr", "").alias("chrom"),
                "start",
                "end",
                "gene_name",
                "score",
            )
            # Lift over to the GRCh38 build:
            .transform(lambda df: lift.convert_intervals(df, "chrom", "start", "end"))
            .alias("intervals")
            # Map gene names to gene IDs:
            .join(
                gene_index.alias("genes"),
                on=[f.col("intervals.gene_name") == f.col("genes.symbols")],
                how="inner",
            )
            # Select relevant columns and add constant columns:
            .select(
                "chromosome",
                f.col("mapped_start").alias("start"),
                f.col("mapped_end").alias("end"),
                "geneId",
                "score",
                f.lit(self.DATASET_NAME).alias("datasetName"),
                f.lit(self.DATA_TYPE).alias("dataType"),
                f.lit(self.EXPERIMENT_TYPE).alias("experimentType"),
                f.lit(self.PMID).alias("pmid"),
                f.lit(self.BIO_FEATURE).alias("bioFeature"),
            )
            .distinct()
            .persist()
        )
        etl.logger.info(f"Number of rows: {self.Thurman_intervals.count()}")

    def get_intervals(self: ParseThurman) -> DataFrame:
        return self.Thurman_intervals

    def qc_intervals(self: ParseThurman) -> None:
        """
        Perform QC on the anderson intervals.
        """

        # Get numbers:
        self.etl.logger.info(f"Size of Thurman data: {self.Thurman_intervals.count()}")
        self.etl.logger.info(
            f'Number of unique intervals: {self.Thurman_intervals.select("start", "end").distinct().count()}'
        )
        self.etl.logger.info(
            f'Number genes in the Thurman dataset: {self.Thurman_intervals.select("geneId").distinct().count()}'
        )
