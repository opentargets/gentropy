from __future__ import annotations

import argparse
import logging

import pandas as pd
import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, dataframe

from intervals.Liftover import LiftOverSpark


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
        anderson_data_file: str,
        gene_index: dataframe,
        lift: LiftOverSpark,
    ) -> None:

        logging.info("Parsing Andersson 2014 data...")
        logging.info(f"Reading data from {anderson_data_file}")

        # Read the anderson file:
        parserd_anderson_df = (
            SparkSession.getActiveSession()
            .createDataFrame(
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

        logging.info(f"Number of rows: {self.anderson_intervals.count()}")

    def get_intervals(self: ParseAndersson) -> dataframe:
        return self.anderson_intervals

    def qc_intervals(self: ParseAndersson) -> None:
        """
        Perform QC on the anderson intervals.
        """

        # Get numbers:
        logging.info(f"Size of Andersson data: {self.anderson_intervals.count()}")
        logging.info(
            f'Number of unique intervals: {self.anderson_intervals.select("start", "end").distinct().count()}'
        )
        logging.info(
            f'Number genes in the Andersson dataset: {self.anderson_intervals.select("gene_id").distinct().count()}'
        )

    def save_parquet(self: ParseAndersson, output_file: str) -> None:
        self.anderson_intervals.write.mode("overwrite").parquet(output_file)


def main(
    anderson_data_file: str, gene_index_file: str, chain_file: str, output_file: str
) -> None:

    spark_conf = (
        SparkConf()
        .set("spark.driver.memory", "10g")
        .set("spark.executor.memory", "10g")
        .set("spark.driver.maxResultSize", "0")
        .set("spark.debug.maxToStringFields", "2000")
        .set("spark.sql.execution.arrow.maxRecordsPerBatch", "500000")
        .set("spark.driver.bindAddress", "127.0.0.1")
    )
    spark = (
        pyspark.sql.SparkSession.builder.config(conf=spark_conf)
        .master("local[*]")
        .getOrCreate()
    )

    logging.info("Reading genes and initializeing liftover.")

    # Initialize LiftOver and gene objects:
    gene_index = spark.read.parquet(gene_index_file)
    lift = LiftOverSpark(chain_file)

    # Initialze the parser:
    logging.info("Starting Andersson data processing.")
    anderson = ParseAndersson(anderson_data_file, gene_index, lift)

    # run QC:
    logging.info("Running QC on the anderson intervals.")
    anderson.qc_intervals()

    # Save data:
    logging.info(f"Saving data to {output_file}.")
    anderson.save_parquet(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wrapper for the the Anderson interval data parser."
    )
    parser.add_argument(
        "--anderson_file", type=str, help="Path to the anderson file (.bed)"
    )
    parser.add_argument(
        "--gene_index", type=str, help="Path to the gene index file (.csv)"
    )
    parser.add_argument(
        "--chain_file", type=str, help="Path to the chain file (.chain)"
    )
    parser.add_argument(
        "--output_file", type=str, help="Path to the output file (.parquet)"
    )
    args = parser.parse_args()

    # Initialize logging:
    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Just print out some of the arguments:
    logging.info(f"Anderson file: {args.anderson_file}")
    logging.info(f"Gene index file: {args.gene_index}")
    logging.info(f"Chain file: {args.chain_file}")
    logging.info(f"Output file: {args.output_file}")

    main(args.anderson_file, args.gene_index, args.chain_file, args.output_file)
