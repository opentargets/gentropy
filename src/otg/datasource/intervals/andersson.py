"""Interval dataset from Andersson et al. 2014."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.assets import schemas
from otg.dataset.intervals import Intervals

if TYPE_CHECKING:
    from otg.common.Liftover import LiftOverSpark
    from otg.common.session import Session
    from otg.dataset.gene_index import GeneIndex


class IntervalsAndersson(Intervals):
    """Interval dataset from Andersson et al. 2014."""

    @classmethod
    def parse(
        cls: type[IntervalsAndersson],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse Andersson et al. 2014 dataset.

        Args:
            session (Session): session
            path (str): Path to dataset
            gene_index (GeneIndex): Gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: Intervals dataset
        """
        # Constant values:
        dataset_name = "andersson2014"
        experiment_type = "fantom5"
        pmid = "24670763"
        bio_feature = "aggregate"
        twosided_threshold = 2.45e6  # <-  this needs to phased out. Filter by percentile instead of absolute value.

        session.logger.info("Parsing Andersson 2014 data...")
        session.logger.info(f"Reading data from {path}")

        # Expected andersson et al. schema:
        input_schema = t.StructType.fromJson(
            json.loads(
                pkg_resources.read_text(schemas, "andersson2014.json", encoding="utf-8")
            )
        )

        # Read the anderson file:
        parsed_anderson_df = (
            session.spark.read.option("delimiter", "\t")
            .option("header", "true")
            .schema(input_schema)
            .csv(path)
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
        )

        return cls(
            _df=(
                # Lift over the intervals:
                lift.convert_intervals(parsed_anderson_df, "chrom", "start", "end")
                .drop("start", "end")
                .withColumnRenamed("mapped_start", "start")
                .withColumnRenamed("mapped_end", "end")
                .distinct()
                # Joining with the gene index
                .alias("intervals")
                .join(
                    gene_index.symbols_lut().alias("genes"),
                    on=[f.col("intervals.gene_symbol") == f.col("genes.geneSymbol")],
                    how="left",
                )
                .filter(
                    # Drop rows where the gene is not on the same chromosome
                    (f.col("chrom") == f.col("chromosome"))
                    # Drop rows where the TSS is far from the start of the region
                    & (
                        f.abs((f.col("start") + f.col("end")) / 2 - f.col("tss"))
                        <= twosided_threshold
                    )
                )
                # Select relevant columns:
                .select(
                    "chromosome",
                    "start",
                    "end",
                    "geneId",
                    "resourceScore",
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                    f.lit(bio_feature).alias("biofeature"),
                )
            ),
            _schema=Intervals.get_schema(),
        )
