from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as f
import pyspark.sql.types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.intervals.Liftover import LiftOverSpark


class ParseAndersson:
    """
    Parse the anderson file and return a dataframe with the intervals.

    :param anderson_file: Path to the anderson file (.bed).
    :param gene_index: PySpark dataframe with the gene index.
    :param lift: LiftOverSpark object.

    **Summary of the logic**

    - Reading .bed file (input)
    - Parsing the names column -> chr, start, end, gene, score
    - Mapping the coordinates to the new build -> liftover
    - Joining with target index by gene symbol (some loss as input uses obsoleted terms)
    - Dropping rows where the gene is on other chromosomes
    - Dropping rows where the gene TSS is too far from the midpoint of the intervals
    - Adding constant columns for this dataset
    """

    # Constant values:
    DATASET_NAME = "andersson2014"
    DATA_TYPE = "interval"
    EXPERIMENT_TYPE = "fantom5"
    PMID = "24670763"
    BIO_FEATURE = "aggregate"
    TWOSIDED_THRESHOLD = 2.45e6  # <-  this needs to phased out. Filter by percentile instead of absolute value.

    def __init__(
        self: ParseAndersson,
        etl: ETLSession,
        anderson_data_file: str,
        gene_index: DataFrame,
        lift: LiftOverSpark,
    ) -> None:

        self.etl = etl

        etl.logger.info("Parsing Andersson 2014 data...")
        etl.logger.info(f"Reading data from {anderson_data_file}")

        # Read the anderson file:
        parserd_anderson_df = (
            etl.spark.createDataFrame(
                pd.read_csv(
                    anderson_data_file, sep="\t", header=0, low_memory=False, skiprows=1
                )
            )
            # Parsing score column and casting as float:
            .withColumn("score", f.col("score").cast("float") / f.lit(1000))
            # Parsing the 'name' column:
            .withColumn("parsedName", f.split(f.col("name"), ";"))
            .withColumn("gene_symbol", f.col("parsedName")[2])
            .withColumn("location", f.col("parsedName")[0])
            .withColumn(
                "chrom",
                f.regexp_replace(f.split(f.col("location"), ":|-")[0], "chr", ""),
            )
            .withColumn(
                "start", f.split(f.col("location"), ":|-")[1].cast(t.IntegerType())
            )
            .withColumn(
                "end", f.split(f.col("location"), ":|-")[2].cast(t.IntegerType())
            )
            # Select relevant columns:
            .select("chrom", "start", "end", "gene_symbol", "score")
            # Drop rows with non-canonical chromosomes:
            .filter(
                f.col("chrom").isin([str(x) for x in range(1, 23)] + ["X", "Y", "MT"])
            )
            # For each region/gene, keep only one row with the highest score:
            .groupBy("chrom", "start", "end", "gene_symbol")
            .agg(f.max("score").alias("score"))
            .orderBy("chrom", "start")
            .persist()
        )

        # Prepare gene set:
        genes = gene_index.withColumnRenamed("gene_name", "gene_symbol").select(
            "gene_symbol", "chr", "gene_id", "TSS"
        )

        self.anderson_intervals = (
            # Lift over the intervals:
            lift.convert_intervals(parserd_anderson_df, "chrom", "start", "end")
            .drop("start", "end")
            .withColumnRenamed("mapped_start", "start")
            .withColumnRenamed("mapped_end", "end")
            .distinct()
            # Joining with the gene index (unfortunately we are losing a bunch of genes here due to old symbols):
            .join(genes, on="gene_symbol", how="left")
            .filter(
                # Drop rows where the gene is not on the same chromosome
                (f.col("chrom") == f.regexp_replace(f.col("chr"), "chr", ""))
                # Drop rows where the TSS is far from the start of the region
                & (
                    f.abs((f.col("start") + f.col("end")) / 2 - f.col("TSS"))
                    <= self.TWOSIDED_THRESHOLD
                )
            )
            # Select relevant columns:
            .select(
                "chrom",
                "start",
                "end",
                "gene_id",
                "score",
                f.lit(self.DATASET_NAME).alias("dataset_name"),
                f.lit(self.DATA_TYPE).alias("data_type"),
                f.lit(self.EXPERIMENT_TYPE).alias("experiment_type"),
                f.lit(self.PMID).alias("pmid"),
                f.lit(self.BIO_FEATURE).alias("bio_feature"),
            )
            .persist()
        )

        etl.logger.info(f"Number of rows: {self.anderson_intervals.count()}")

    def get_intervals(self: ParseAndersson) -> DataFrame:
        return self.anderson_intervals

    def qc_intervals(self: ParseAndersson) -> None:
        """
        Perform QC on the anderson intervals.
        """

        # Get numbers:
        self.etl.logger.info(
            f"Size of Andersson data: {self.anderson_intervals.count()}"
        )
        self.etl.logger.info(
            f'Number of unique intervals: {self.anderson_intervals.select("start", "end").distinct().count()}'
        )
        self.etl.logger.info(
            f'Number genes in the Andersson dataset: {self.anderson_intervals.select("gene_id").distinct().count()}'
        )
