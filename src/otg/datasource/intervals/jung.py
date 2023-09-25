"""Interval dataset from Jung et al. 2019."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t

from otg.dataset.intervals import Intervals

if TYPE_CHECKING:
    from otg.common.Liftover import LiftOverSpark
    from otg.common.session import Session
    from otg.dataset.gene_index import GeneIndex


class IntervalsJung(Intervals):
    """Interval dataset from Jung et al. 2019."""

    @classmethod
    def parse(
        cls: type[IntervalsJung],
        session: Session,
        path: str,
        gene_index: GeneIndex,
        lift: LiftOverSpark,
    ) -> Intervals:
        """Parse the Jung et al. 2019 dataset.

        Args:
            session (Session): session
            path (str): path to the Jung et al. 2019 dataset
            gene_index (GeneIndex): gene index
            lift (LiftOverSpark): LiftOverSpark instance

        Returns:
            Intervals: _description_
        """
        dataset_name = "javierre2016"
        experiment_type = "pchic"
        pmid = "27863249"

        session.logger.info("Parsing Jung 2019 data...")
        session.logger.info(f"Reading data from {path}")

        # Read Jung data:
        jung_raw = (
            session.spark.read.csv(path, sep=",", header=True)
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
        )

        # Lifting over the coordinates:
        return cls(
            _df=(
                jung_raw
                # Lifting over to GRCh38 interval 1:
                .transform(
                    lambda df: lift.convert_intervals(df, "chrom", "start", "end")
                )
                .select(
                    "chrom",
                    f.col("mapped_start").alias("start"),
                    f.col("mapped_end").alias("end"),
                    f.explode(f.split(f.col("gene_name"), ";")).alias("gene_name"),
                    "tissue",
                )
                .alias("intervals")
                # Joining with genes:
                .join(
                    gene_index.symbols_lut().alias("genes"),
                    on=[f.col("intervals.gene_name") == f.col("genes.geneSymbol")],
                    how="inner",
                )
                # Finalize dataset:
                .select(
                    "chromosome",
                    "start",
                    "end",
                    "geneId",
                    f.col("tissue").alias("biofeature"),
                    f.lit(1.0).alias("score"),
                    f.lit(dataset_name).alias("datasourceId"),
                    f.lit(experiment_type).alias("datatypeId"),
                    f.lit(pmid).alias("pmid"),
                )
                .drop_duplicates()
            ),
            _schema=Intervals.get_schema(),
        )
