"""PCHi-C intervals (Javierre et al. 2016).

Javierre and colleagues uses promoter capture Hi-C to identify interacting regions of 31,253 promoters in 17 human primary hematopoietic cell types. ([Link](https://www.sciencedirect.com/science/article/pii/S0092867416313228) to the publication)

The dataset provides cell type resolution, however these cell types are not resolved to tissues. Scores are also preserved.

"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession
    from etl.intervals.Liftover import LiftOverSpark


class ParseJavierre:
    """Javierre 2016 dataset parsers.

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
        etl: ETLSession,
        javierre_parquet: str,
        gene_index: DataFrame,
        lift: LiftOverSpark,
    ) -> None:
        """Initialise Javierre parser.

        Args:
            etl (ETLSession): ETL session
            javierre_parquet (str): path to the parquet file containing the Javierre 2016 data after processing it (see notebooks/Javierre_data_pre-process.ipynb)
            gene_index (DataFrame): Pyspark dataframe containing the gene index
            lift (LiftOverSpark): LiftOverSpark object
        """
        self.etl = etl

        etl.logger.info("Parsing Javierre 2016 data...")
        etl.logger.info(f"Reading data from {javierre_parquet}")

        # Read Javierre data:
        javierre_raw = (
            etl.spark.read.parquet(javierre_parquet)
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
            javierre_remapped.alias("intervals")
            .select(
                f.col("chrom"),
                f.col("start").cast(t.IntegerType()),
                f.col("end").cast(t.IntegerType()),
            )
            .distinct()
            .join(
                gene_index.alias("genes"),
                on=[f.col("intervals.chrom") == f.col("genes.chromosome")],
                how="left",
            )
            .filter(
                (
                    (f.col("start") >= f.col("genomicLocation.start"))
                    & (f.col("start") <= f.col("genomicLocation.end"))
                )
                | (
                    (f.col("end") >= f.col("genomicLocation.start"))
                    & (f.col("end") <= f.col("genomicLocation.end"))
                )
            )
            .select("chrom", "start", "end", "geneId", "tss")
        )

        # Joining back the data:
        self.javierre_intervals = (
            javierre_remapped.join(
                unique_intervals_with_genes, on=["chrom", "start", "end"], how="left"
            )
            .filter(
                # Drop rows where the TSS is far from the start of the region
                f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                <= self.TWOSIDED_THRESHOLD
            )
            # For each gene, keep only the highest scoring interval:
            .groupBy(
                "name_chr", "name_start", "name_end", "genes.geneId", "bio_feature"
            )
            .agg(f.max(f.col("name_score")).alias("score"))
            # Create the output:
            .select(
                f.col("name_chr").alias("chromosome"),
                f.col("name_start").alias("start"),
                f.col("name_end").alias("end"),
                f.col("score"),
                f.col("genes.geneId").alias("geneId"),
                f.col("bio_feature").alias("bioFeature"),
                f.lit(self.DATASET_NAME).alias("datasetName"),
                f.lit(self.DATA_TYPE).alias("dataType"),
                f.lit(self.EXPERIMENT_TYPE).alias("experimentType"),
                f.lit(self.PMID).alias("pmid"),
            )
            .persist()
        )
        etl.logger.info(f"Number of rows: {self.javierre_intervals.count()}")

    def get_intervals(self: ParseJavierre) -> DataFrame:
        """Get preformatted Javierre intervals."""
        return self.javierre_intervals

    def qc_intervals(self: ParseJavierre) -> None:
        """Perform QC on the anderson intervals."""
        # Get numbers:
        self.etl.logger.info(
            f"Size of Javierre data: {self.javierre_intervals.count()}"
        )
        self.etl.logger.info(
            f'Number of unique intervals: {self.javierre_intervals.select("start", "end").distinct().count()}'
        )
        self.etl.logger.info(
            f'Number genes in the Javierre dataset: {self.javierre_intervals.select("geneId").distinct().count()}'
        )
