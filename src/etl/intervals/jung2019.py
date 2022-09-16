from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.intervals.Liftover import LiftOverSpark


class ParseJung:
    """
    Parser Jung 2019 dataset

    :param jung_data: path to the csv file containing the Jung 2019 data
    :param gene_index: Pyspark dataframe containing the gene index
    :param lift: LiftOverSpark object

    **Summary of the logic:**

    - Reading dataset into a PySpark dataframe.
    - Select relevant columns, parsing: start, end.
    - Lifting over the intervals.
    - Split gene names (separated by ;)
    - Look up gene names to gene identifiers.
    """

    # Constants:
    DATASET_NAME = "jung2019"
    DATA_TYPE = "interval"
    EXPERIMENT_TYPE = "pchic"
    PMID = "31501517"

    def __init__(
        self: ParseJung,
        etl: ETLSession,
        jung_data: str,
        gene_index: DataFrame,
        lift: LiftOverSpark,
    ) -> None:

        etl.logger.info("Parsing Jung 2019 data...")
        etl.logger.info(f"Reading data from {jung_data}")

        self.etl = etl

        # Read Jung data:
        jung_raw = (
            etl.spark.read.csv(jung_data, sep=",", header=True)
            .withColumn("interval", f.split(f.col("Interacting_fragment"), r"\."))
            .select(
                # Parsing intervals:
                f.regexp_replace(f.col("interval")[0], "chr", "").alias("chrom"),
                f.col("interval")[1].cast(t.IntegerType()).alias("start"),
                f.col("interval")[2].cast(t.IntegerType()).alias("end"),
                # Extract other columns:
                f.col("Promoter").alias("gene_name"),
                f.col("Tissue_type").alias("tissue"),
            )
            .persist()
        )

        # Lifting over the coordinates:
        self.jung_intervals = (
            jung_raw
            # Lifting over to GRCh38 interval 1:
            .transform(lambda df: lift.convert_intervals(df, "chrom", "start", "end"))
            .select(
                "chrom",
                f.col("mapped_start").alias("start"),
                f.col("mapped_end").alias("end"),
                f.explode(f.split(f.col("gene_name"), ";")).alias("gene_name"),
                "tissue",
            )
            # Joining with genes:
            .join(
                gene_index.select("gene_name", "gene_id"), on="gene_name", how="inner"
            )
            # Finalize dataset:
            .select(
                "chrom",
                "start",
                "end",
                "gene_id",
                "tissue",
                f.lit(self.DATASET_NAME).alias("dataset_name"),
                f.lit(self.DATA_TYPE).alias("data_type"),
                f.lit(self.EXPERIMENT_TYPE).alias("experiment_type"),
                f.lit(self.PMID).alias("pmid"),
            )
            .drop_duplicates()
            .persist()
        )

        etl.logger.info(f"Number of rows: {self.jung_intervals.count()}")

    def get_intervals(self: ParseJung) -> DataFrame:
        return self.jung_intervals

    def qc_intervals(self: ParseJung) -> None:
        """
        Perform QC on the Jung intervals.
        """

        # Get numbers:
        self.etl.logger.info(f"Size of Jung data: {self.jung_intervals.count()}")
        self.etl.logger.info(
            f'Number of unique intervals: {self.jung_intervals.select("start", "end").distinct().count()}'
        )
        self.etl.logger.info(
            f'Number genes in the Jung dataset: {self.jung_intervals.select("gene_id").distinct().count()}'
        )
