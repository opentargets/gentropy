from __future__ import annotations

import argparse
import logging

import pyspark
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, dataframe

from intervals.Liftover import LiftOverSpark


class ParseJavierre:
    """
    Parser Javierre 2016 dataset

    :param javierre_parquet: path to the parquet file containing the Javierre 2016 data after processing it (see notebooks/Javierre_data_pre-process.ipynb)
    :param gene_index: Pyspark dataframe containing the gene index
    :param lift: LiftOverSpark object

    **Summary of the logic:**

    - Reading parquet file containing the pre-processed Javierre dataset.
    - Splitting name column into chromosome, start, end, and score.
    - Lifting over the intervals.
    - Mapping intervals to genes by overlapping regions.
    - For each gene/interval pair, keep only the highest scoring interval.
    - Filter gene/interval pairs by the distance between the TSS and the start of the interval.
    """

    # Constants:
    DATASET_NAME = "javierre2016"
    DATA_TYPE = "interval"
    EXPERIMENT_TYPE = "pchic"
    PMID = "27863249"
    TWOSIDED_THRESHOLD = 2.45e6  # <-  this needs to phased out. Filter by percentile instead of absolute value.

    def __init__(
        self: ParseJavierre,
        javierre_parquet: str,
        gene_index: dataframe,
        lift: LiftOverSpark,
    ) -> None:

        logging.info("Parsing Javierre 2016 data...")
        logging.info(f"Reading data from {javierre_parquet}")

        # Read gene index:
        genes = self.prepare_gene_index(gene_index)

        # Read Javierre data:
        javierre_raw = (
            SparkSession.getActiveSession()
            .read.parquet(javierre_parquet)
            # Splitting name column into chromosome, start, end, and score:
            .withColumn("name_split", f.split(f.col("name"), r":|-|,"))
            .withColumn(
                "name_chr",
                f.regexp_replace(f.col("name_split")[0], "chr", "").cast(
                    t.StringType()
                ),
            )
            .withColumn("name_start", f.col("name_split")[1].cast(t.IntegerType()))
            .withColumn("name_end", f.col("name_split")[2].cast(t.IntegerType()))
            .withColumn("name_score", f.col("name_split")[3].cast(t.FloatType()))
            # Cleaning up chromosome:
            .withColumn(
                "chrom",
                f.regexp_replace(f.col("chrom"), "chr", "").cast(t.StringType()),
            )
            .drop("name_split", "name", "annotation")
            # Keep canonical chromosomes and consistent chromosomes with scores:
            .filter(
                (f.col("name_score").isNotNull())
                & (f.col("chrom") == f.col("name_chr"))
                & f.col("name_chr").isin(
                    [f"{x}" for x in range(1, 23)] + ["X", "Y", "MT"]
                )
            )
        )

        # Lifting over intervals:
        javierre_remapped = (
            javierre_raw
            # Lifting over to GRCh38 interval 1:
            .transform(lambda df: lift.convert_intervals(df, "chrom", "start", "end"))
            .drop("start", "end")
            .withColumnRenamed("mapped_chrom", "chrom")
            .withColumnRenamed("mapped_start", "start")
            .withColumnRenamed("mapped_end", "end")
            # Lifting over interval 2 to GRCh38:
            .transform(
                lambda df: lift.convert_intervals(
                    df, "name_chr", "name_start", "name_end"
                )
            )
            .drop("name_start", "name_end")
            .withColumnRenamed("mapped_name_chr", "name_chr")
            .withColumnRenamed("mapped_name_start", "name_start")
            .withColumnRenamed("mapped_name_end", "name_end")
            .persist()
        )

        # Once the intervals are lifted, extracting the unique intervals:
        unique_intervals_with_genes = (
            javierre_remapped.select(
                "chrom",
                f.col("start").cast(t.IntegerType()),
                f.col("end").cast(t.IntegerType()),
            )
            .distinct()
            .join(genes, on=["chrom"], how="left")
            .filter(
                (
                    (f.col("start") >= f.col("gene_start"))
                    & (f.col("start") <= f.col("gene_end"))
                )
                | (
                    (f.col("end") >= f.col("gene_start"))
                    & (f.col("end") <= f.col("gene_end"))
                )
            )
            .select("chrom", "start", "end", "gene_id", "TSS")
        )

        # Joining back the data:
        self.javierre_intervals = (
            javierre_remapped.join(
                unique_intervals_with_genes, on=["chrom", "start", "end"], how="left"
            )
            .filter(
                # Drop rows where the TSS is far from the start of the region
                f.abs((f.col("start") + f.col("end")) / 2 - f.col("TSS"))
                <= self.TWOSIDED_THRESHOLD
            )
            # For each gene, keep only the highest scoring interval:
            .groupBy("name_chr", "name_start", "name_end", "gene_id", "bio_feature")
            .agg(f.max(f.col("name_score")).alias("score"))
            # Create the output:
            .select(
                f.col("name_chr").alias("chrom"),
                f.col("name_start").alias("start"),
                f.col("name_end").alias("end"),
                f.col("score"),
                f.col("gene_id").alias("gene_id"),
                f.col("bio_feature").alias("cell_type"),
                f.lit(self.DATASET_NAME).alias("dataset_name"),
                f.lit(self.DATA_TYPE).alias("data_type"),
                f.lit(self.EXPERIMENT_TYPE).alias("experiment_type"),
                f.lit(self.PMID).alias("pmid"),
            )
            .persist()
        )
        logging.info(f"Number of rows: {self.javierre_intervals.count()}")

    def get_intervals(self: ParseJavierre) -> dataframe:
        return self.javierre_intervals

    def qc_intervals(self: ParseJavierre) -> None:
        """
        Perform QC on the anderson intervals.
        """

        # Get numbers:
        logging.info(f"Size of Javierre data: {self.javierre_intervals.count()}")
        logging.info(
            f'Number of unique intervals: {self.javierre_intervals.select("start", "end").distinct().count()}'
        )
        logging.info(
            f'Number genes in the Javierre dataset: {self.javierre_intervals.select("gene_id").distinct().count()}'
        )

    def save_parquet(self: ParseJavierre, output_file: str) -> None:
        self.javierre_intervals.write.mode("overwrite").parquet(output_file)

    @staticmethod
    def prepare_gene_index(gene_index: dataframe) -> dataframe:
        """Pre-processing the gene dataset
        - selecting and renaming relevant columns
        - remove 'chr' from chromosome column

        :param gene_index: Path to the gene parquet file
        :return: Spark Dataframe
        """
        # Reading gene annotations:
        return gene_index.select(
            f.regexp_replace(f.col("chr"), "chr", "").alias("chrom"),
            f.col("start").cast(t.IntegerType()).alias("gene_start"),
            f.col("end").cast(t.IntegerType()).alias("gene_end"),
            "gene_id",
            "TSS",
        ).persist()


def main(
    javierre_data_file: str, gene_index_file: str, chain_file: str, output_file: str
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
    logging.info("Starting Javierre data processing.")
    javierre = ParseJavierre(javierre_data_file, gene_index, lift)

    # run QC:
    logging.info("Running QC on the intervals.")
    javierre.qc_intervals()

    # Save data:
    logging.info(f"Saving data to {output_file}.")
    javierre.save_parquet(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Wrapper for the the Javierre interval data parser."
    )
    parser.add_argument(
        "--javierre_file",
        type=str,
        help="Path to the pre-processed parquet dataset (.parquet).",
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
    logging.info(f"Javierre file: {args.javierre_file}")
    logging.info(f"Gene index file: {args.gene_index}")
    logging.info(f"Chain file: {args.chain_file}")
    logging.info(f"Output file: {args.output_file}")

    main(args.javierre_file, args.gene_index, args.chain_file, args.output_file)
