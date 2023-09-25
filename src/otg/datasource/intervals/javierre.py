"""Interval dataset from Javierre et al. 2016."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.dataset.intervals import Intervals

if TYPE_CHECKING:
    from otg.common.Liftover import LiftOverSpark
    from otg.common.session import Session
    from otg.dataset.gene_index import GeneIndex


class IntervalsJavierre(Intervals):
    """Interval dataset from Javierre et al. 2016."""

    @classmethod
    def parse(
        cls: type[IntervalsJavierre],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse Javierre et al. 2016 dataset.

        Args:
            session (Session): session
            path (str): Path to dataset
            gene_index (GeneIndex): Gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: Javierre et al. 2016 interval data
        """
        # Constant values:
        dataset_name = "javierre2016"
        experiment_type = "pchic"
        pmid = "27863249"
        twosided_threshold = 2.45e6

        session.logger.info("Parsing Javierre 2016 data...")
        session.logger.info(f"Reading data from {path}")

        # Read Javierre data:
        javierre_raw = (
            session.spark.read.parquet(path)
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
                gene_index.locations_lut().alias("genes"),
                on=[f.col("intervals.chrom") == f.col("genes.chromosome")],
                how="left",
            )
            # TODO: add filter as part of the join condition
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
        return cls(
            _df=(
                javierre_remapped.join(
                    unique_intervals_with_genes,
                    on=["chrom", "start", "end"],
                    how="left",
                )
                .filter(
                    # Drop rows where the TSS is far from the start of the region
                    f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                    <= twosided_threshold
                )
                # For each gene, keep only the highest scoring interval:
                .groupBy(
                    "name_chr", "name_start", "name_end", "genes.geneId", "bio_feature"
                )
                .agg(f.max(f.col("name_score")).alias("resourceScore"))
                # Create the output:
                .select(
                    f.col("name_chr").alias("chromosome"),
                    f.col("name_start").alias("start"),
                    f.col("name_end").alias("end"),
                    f.col("resourceScore"),
                    f.col("genes.geneId").alias("geneId"),
                    f.col("bio_feature").alias("biofeature"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
