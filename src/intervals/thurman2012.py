from __future__ import annotations

import argparse
import logging

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, dataframe

from intervals.Liftover import LiftOverSpark


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
        thurman_datafile: str,
        gene_index: dataframe,
        lift: LiftOverSpark,
    ) -> None:

        logging.info("Parsing Thurman 2012 data...")
        logging.info(f"Reading data from {thurman_datafile}")

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
            SparkSession.getActiveSession()
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
            # Map gene names to gene IDs:
            .join(
                gene_index.select("gene_id", "gene_name"), how="inner", on="gene_name"
            )
            # Select relevant columns and add constant columns:
            .select(
                "chrom",
                f.col("mapped_start").alias("start"),
                f.col("mapped_end").alias("end"),
                "gene_id",
                "score",
                f.lit(self.DATASET_NAME).alias("dataset_name"),
                f.lit(self.DATA_TYPE).alias("data_type"),
                f.lit(self.EXPERIMENT_TYPE).alias("experiment_type"),
                f.lit(self.PMID).alias("pmid"),
                f.lit(self.BIO_FEATURE).alias("bio_feature"),
            )
            .distinct()
            .persist()
        )
        logging.info(f"Number of rows: {self.Thurman_intervals.count()}")

    def get_intervals(self: ParseThurman) -> dataframe:
        return self.Thurman_intervals

    def qc_intervals(self: ParseThurman) -> None:
        """
        Perform QC on the anderson intervals.
        """

        # Get numbers:
        logging.info(f"Size of Thurman data: {self.Thurman_intervals.count()}")
        logging.info(
            f'Number of unique intervals: {self.Thurman_intervals.select("start", "end").distinct().count()}'
        )
        logging.info(
            f'Number genes in the Thurman dataset: {self.Thurman_intervals.select("gene_id").distinct().count()}'
        )

    def save_parquet(self: ParseThurman, output_file: str) -> None:
        self.Thurman_intervals.write.mode("overwrite").parquet(output_file)


def main(
    thurman_data_file: str, gene_index_file: str, chain_file: str, output_file: str
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

    logging.info("Reading genes and initializing liftover.")

    # Initialize LiftOver and gene objects:
    gene_index = spark.read.parquet(gene_index_file)
    lift = LiftOverSpark(chain_file)

    # Initialze the parser:
    logging.info("Starting Thurman data processing.")
    thurman = ParseThurman(thurman_data_file, gene_index, lift)

    # run QC:
    logging.info("Running QC on the intervals.")
    thurman.qc_intervals()

    # Save data:
    logging.info(f"Saving data to {output_file}.")
    thurman.save_parquet(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wrapper for the the Thurman interval data parser."
    )
    parser.add_argument(
        "--thurman_file", type=str, help="Path to the tsv dataset (.tsv)."
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
    logging.info(f"Thurman file: {args.thurman_file}")
    logging.info(f"Gene index file: {args.gene_index}")
    logging.info(f"Chain file: {args.chain_file}")
    logging.info(f"Output file: {args.output_file}")

    main(args.thurman_file, args.gene_index, args.chain_file, args.output_file)
