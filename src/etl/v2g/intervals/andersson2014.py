"""Enhancer-TSS correlation (Andersson et al. 2014) intervals.

As part of the [FANTOM5](https://fantom.gsc.riken.jp/5/) genome mapping effort, this publication aims to report on actively transcribed enhancers from the majority of human tissues. ([Link](https://www.nature.com/articles/nature12787) to the publication)

The dataset is not allows to resolve individual tissues, the biotype is `aggregate`. However the aggregation provides a score quantifying the association of the genomic region and the gene.

"""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from etl.json import schemas

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.v2g.intervals.Liftover import LiftOverSpark


class ParseAndersson:
    """Parse the anderson file and return a dataframe with the intervals.

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
        """Intialise Andersson parser.

        Args:
            etl (ETLSession): Spark session
            anderson_data_file (str): Anderson et al. filepath
            gene_index (DataFrame): gene index information
            lift (LiftOverSpark): LiftOverSpark object
        """
        self.etl = etl

        etl.logger.info("Parsing Andersson 2014 data...")
        etl.logger.info(f"Reading data from {anderson_data_file}")

        # Expected andersson et al. schema:
        schema = t.StructType.fromJson(
            json.loads(
                pkg_resources.read_text(schemas, "andersson2014.json", encoding="utf-8")
            )
        )

        # Read the anderson file:
        parsed_anderson_df = (
            etl.spark.read.option("delimiter", "\t")
            .option("header", "true")
            .schema(schema)
            .csv(anderson_data_file)
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
            .agg(f.max("score").alias("resourceScore"))
            .orderBy("chrom", "start")
            .persist()
        )

        self.anderson_intervals = (
            # Lift over the intervals:
            lift.convert_intervals(parsed_anderson_df, "chrom", "start", "end")
            .drop("start", "end")
            .withColumnRenamed("mapped_start", "start")
            .withColumnRenamed("mapped_end", "end")
            .distinct()
            # Joining with the gene index
            .alias("intervals")
            .join(
                gene_index.alias("genes"),
                on=[f.col("intervals.gene_symbol") == f.col("genes.symbols")],
                how="left",
            )
            .filter(
                # Drop rows where the gene is not on the same chromosome
                (f.col("chrom") == f.col("chromosome"))
                # Drop rows where the TSS is far from the start of the region
                & (
                    f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                    <= self.TWOSIDED_THRESHOLD
                )
            )
            # Select relevant columns:
            .select(
                "chromosome",
                "start",
                "end",
                "geneId",
                "resourceScore",
                f.lit(self.DATASET_NAME).alias("datasourceId"),
                f.lit(self.EXPERIMENT_TYPE).alias("datatypeId"),
                f.lit(self.PMID).alias("pmid"),
                f.lit(self.BIO_FEATURE).alias("biofeature"),
            )
            .persist()
        )

    def get_intervals(self: ParseAndersson) -> DataFrame:
        """Get formatted interval data."""
        return self.anderson_intervals

    def qc_intervals(self: ParseAndersson) -> None:
        """Perform QC on the anderson intervals."""
        # Get numbers:
        self.etl.logger.info(
            f"Size of Andersson data: {self.anderson_intervals.count()}"
        )
        self.etl.logger.info(
            f'Number of unique intervals: {self.anderson_intervals.select("start", "end").distinct().count()}'
        )
        self.etl.logger.info(
            f'Number genes in the Andersson dataset: {self.anderson_intervals.select("geneId").distinct().count()}'
        )
