from __future__ import annotations

import argparse
import logging

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, dataframe

from intervals.Liftover import LiftOverSpark


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
        self: ParseJung, jung_data: str, gene_index: dataframe, lift: LiftOverSpark
    ) -> None:

        logging.info("Parsing Jung 2019 data...")
        logging.info(f"Reading data from {jung_data}")

        # Read Jung data:
        jung_raw = (
            SparkSession.getActiveSession()
            .read.csv(jung_data, sep=",", header=True)
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

        logging.info(f"Number of rows: {self.jung_intervals.count()}")

    def get_intervals(self: ParseJung) -> dataframe:
        return self.jung_intervals

    def qc_intervals(self: ParseJung) -> None:
        """
        Perform QC on the Jung intervals.
        """

        # Get numbers:
        logging.info(f"Size of Jung data: {self.jung_intervals.count()}")
        logging.info(
            f'Number of unique intervals: {self.jung_intervals.select("start", "end").distinct().count()}'
        )
        logging.info(
            f'Number genes in the Jung dataset: {self.jung_intervals.select("gene_id").distinct().count()}'
        )

    def save_parquet(self: ParseJung, output_file: str) -> None:
        self.jung_intervals.write.mode("overwrite").parquet(output_file)


def main(
    jung_data_file: str, gene_index_file: str, chain_file: str, output_file: str
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
    logging.info("Starting Jung 2019 data processing.")
    jung = ParseJung(jung_data_file, gene_index, lift)

    # run QC:
    logging.info("Running QC on the intervals.")
    jung.qc_intervals()

    # Save data:
    logging.info(f"Saving data to {output_file}.")
    jung.save_parquet(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Wrapper for the the Jung interval data parser.")
    parser.add_argument(
        "--jung_file", type=str, help="Path to the raw csv dataset (.csv)."
    )
    parser.add_argument(
        "--gene_index", type=str, help="Path to the gene index file (.parquet)"
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
    logging.info(f"Jung csv input file: {args.jung_file}")
    logging.info(f"Gene index file: {args.gene_index}")
    logging.info(f"Chain file: {args.chain_file}")
    logging.info(f"Output file: {args.output_file}")

    main(args.jung_file, args.gene_index, args.chain_file, args.output_file)
