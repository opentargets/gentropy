"""LD indexing classes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import (
    get_record_with_maximum_value,
    get_record_with_minimum_value,
)
from otg.common.utils import convert_gnomad_position_to_ensembl
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from otg.common.session import ETLSession
    from pyspark.sql.types import StructType
    from hail.table import Table
    from pyspark.sql import Column

import hail as hl


@dataclass
class LDIndex(Dataset):
    """Dataset to index access to LD information from GnomAD."""

    schema: StructType = parse_spark_schema("ld_index.json")

    @staticmethod
    def _liftover_loci(variant_index: Table, grch37_to_grch38_chain_path: str) -> Table:
        """Liftover hail table with LD variant index.

        Args:
            variant_index (Table): LD variant indexes
            grch37_to_grch38_chain_path (str): Path to chain file

        Returns:
            Table: LD variant index with locus 38 coordinates
        """
        if not hl.get_reference("GRCh37").has_liftover("GRCh38"):
            rg37 = hl.get_reference("GRCh37")
            rg38 = hl.get_reference("GRCh38")
            rg37.add_liftover(grch37_to_grch38_chain_path, rg38)

        return variant_index.annotate(
            locus38=hl.liftover(variant_index.locus, "GRCh38")
        )

    @staticmethod
    def _interval_start(contig: Column, position: Column, ld_radius: int) -> Column:
        """Start position of the interval based on available positions.

        Args:
            contig (Column): genomic contigs
            position (Column): genomic positions
            ld_radius (int): bp around locus

        Returns:
            Column: Position of the locus starting the interval

        Examples:
            >>> d = [
            ...     {"contig": "21", "pos": 100},
            ...     {"contig": "21", "pos": 200},
            ...     {"contig": "21", "pos": 300},
            ... ]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("start", _interval_start(f.col("contig"), f.col("pos"), 100)).show()
            +------+---+-----+
            |contig|pos|start|
            +------+---+-----+
            |    21|100|  100|
            |    21|200|  100|
            |    21|300|  200|
            +------+---+-----+
            <BLANKLINE>

        """
        w = (
            Window.partitionBy(contig)
            .orderBy(position)
            .rangeBetween(-ld_radius, ld_radius)
        )
        return f.min(position).over(w)

    @staticmethod
    def _interval_stop(contig: Column, position: Column, ld_radius: int) -> Column:
        """Stop position of the interval based on available positions.

        Args:
            contig (Column): genomic contigs
            position (Column): genomic positions
            ld_radius (int): bp around locus

        Returns:
            Column: Position of the locus at the end of the interval

        Examples:
            >>> d = [
            ...     {"contig": "21", "pos": 100},
            ...     {"contig": "21", "pos": 200},
            ...     {"contig": "21", "pos": 300},
            ... ]
            >>> df = spark.createDataFrame(d)
            >>> df.withColumn("start", _interval_stop(f.col("contig"), f.col("pos"), 100)).show()
            +------+---+-----+
            |contig|pos|start|
            +------+---+-----+
            |    21|100|  200|
            |    21|200|  300|
            |    21|300|  300|
            +------+---+-----+
            <BLANKLINE>

        """
        w = (
            Window.partitionBy(contig)
            .orderBy(position)
            .rangeBetween(-ld_radius, ld_radius)
        )
        return f.max(position).over(w)

    @classmethod
    def from_parquet(cls: type[LDIndex], etl: ETLSession, path: str) -> LDIndex:
        """Initialise LD index from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            LDIndex: LD index dataset
        """
        return super().from_parquet(etl, path, cls.schema)

    @classmethod
    def create(
        cls: type[LDIndex],
        pop_ldindex_path: str,
        ld_radius: int,
        grch37_to_grch38_chain_path: str,
    ) -> LDIndex:
        """Parse LD index and annotate with interval start and stop.

        Args:
            pop_ldindex_path (str): path to gnomAD LD index
            ld_radius (int): radius
            grch37_to_grch38_chain_path (str): path to chain file for liftover

        Returns:
            LDIndex: Created GnomAD LD index
        """
        ld_index = hl.read_table(pop_ldindex_path).naive_coalesce(400)
        ld_index_38 = LDIndex._liftover_loci(ld_index, grch37_to_grch38_chain_path)

        return cls(
            _df=ld_index_38.to_spark()
            .filter(f.col("`locus38.position`").isNotNull())
            .select(
                f.col("idx"),
                f.regexp_replace("`locus38.contig`", "chr", "").alias("chromosome"),
                f.col("`locus38.position`").alias("position"),
                f.col("`alleles`").getItem(0).alias("referenceAllele"),
                f.col("`alleles`").getItem(1).alias("alternateAllele"),
            )
            .withColumn(
                "position",
                convert_gnomad_position_to_ensembl(
                    f.col("position"),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                ),
            )
            .withColumn(
                "variantId",
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                ),
            )
            # Convert gnomad position to Ensembl position (1-based for indels)
            .repartition(400, "chromosome")
            .sortWithinPartitions("position")
            .persist()
        ).annotate_index_intervals(ld_radius)

    def annotate_index_intervals(self: LDIndex, ld_radius: int) -> LDIndex:
        """Annotate LD index with indexes starting and stopping a given interval.

        Args:
            ld_radius (int): radius around each position

        Returns:
            LDIndex: including `start_idx` and `stop_idx` columns
        """
        index_with_positions = self._df.select(
            "*",
            LDIndex._interval_start(
                contig=f.col("chromosome"),
                position=f.col("position"),
                ld_radius=ld_radius,
            ).alias("start_pos"),
            LDIndex._interval_stop(
                contig=f.col("chromosome"),
                position=f.col("position"),
                ld_radius=ld_radius,
            ).alias("stop_pos"),
        ).persist()

        self.df = (
            index_with_positions.join(
                (
                    index_with_positions
                    # Given the multiple variants with the same chromosome/position can have different indexes, filter for the lowest index:
                    .transform(
                        lambda df: get_record_with_minimum_value(
                            df, ["chromosome", "position"], "idx"
                        )
                    ).select(
                        "chromosome",
                        f.col("position").alias("start_pos"),
                        f.col("idx").alias("start_idx"),
                    )
                ),
                on=["chromosome", "start_pos"],
            )
            .join(
                (
                    index_with_positions
                    # Given the multiple variants with the same chromosome/position can have different indexes, filter for the highest index:
                    .transform(
                        lambda df: get_record_with_maximum_value(
                            df, ["chromosome", "position"], "idx"
                        )
                    ).select(
                        "chromosome",
                        f.col("position").alias("stop_pos"),
                        f.col("idx").alias("stop_idx"),
                    )
                ),
                on=["chromosome", "stop_pos"],
            )
            .drop("start_pos", "stop_pos")
        )

        return self
