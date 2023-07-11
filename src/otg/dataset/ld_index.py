"""Step to dump a filtered version of a LD matrix (block matrix) as Parquet files."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import hail as hl
from hail.linalg import BlockMatrix
from pyspark.sql import Window
import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.utils import _liftover_loci, convert_gnomad_position_to_ensembl
from otg.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session


@dataclass
class LDSet(Dataset):
    """
    Dataset that defines a set of variants correlated by LD across populations.
    The information comes from LD matrices made available by GnomAD.
    """

    _schema: StructType = parse_spark_schema("ld_set.json")

    @staticmethod
    def _convert_ld_matrix_to_table(
        block_matrix: BlockMatrix, min_r2: float
    ) -> DataFrame:
        """Convert LD matrix to table."""
        table = block_matrix.entries(keyed=False)
        return LDSet._transpose_ld_matrix(
            table.filter(hl.abs(table.entry) >= min_r2**0.5)
            .to_spark()
            .withColumnRenamed("entry", "r")
        )

    @staticmethod
    def _transpose_ld_matrix(ld_matrix: DataFrame) -> DataFrame:
        """Transpose LD matrix.
        # TODO: add doctest
        """
        ld_matrix_transposed = ld_matrix.selectExpr("i as j", "j as i", "r")
        return ld_matrix.filter(f.col("i") == f.col("j")).unionByName(
            ld_matrix_transposed
        )

    @staticmethod
    def _process_variant_indices(ld_index_path: str, grch37_to_grch38_chain_path: str):
        """Creates a look up table between variants and their coordinates in the LD Matrix.

        !!! info "Gnomad's LD Matrix and Index are based on GRCh37 coordinates. This function will lift over the coordinates to GRCh38 to build the lookup table."
        """
        ld_index = hl.read_table(ld_index_path).naive_coalesce(400)
        ld_index_38 = _liftover_loci(ld_index, grch37_to_grch38_chain_path, "GRCh38")

        return (
            ld_index_38.to_spark()
            # Filter out variants where the liftover failed
            .filter(f.col("`locus_GRCh38.position`").isNotNull())
            .select(
                f.concat_ws(
                    "_",
                    # parse chromosome
                    f.regexp_replace("`locus_GRCh38.contig`", "chr", ""),
                    # parse position
                    convert_gnomad_position_to_ensembl(
                        f.col("`locus_GRCh38.position`"),
                        f.col("`alleles`").getItem(0),
                        f.col("`alleles`").getItem(1),
                    ),
                    # parse ref and alt alleles
                    f.col("`alleles`").getItem(0),
                    f.col("`alleles`").getItem(1),
                ).alias("variantId"),
                f.col("idx"),
            )
            # Filter out ambiguous liftover results: multiple indices for the same variant
            .withColumn("count", f.count("*").over(Window.partitionBy(["variantId"])))
            .filter(f.col("count") == 1)
            .drop("count")
            .repartition(400, "chromosome")
            .sortWithinPartitions("position")
        )

    @classmethod
    def create(
        cls: type[LDSet],
        ld_matrix_path: str,
        ld_index_path: str,
        grch37_to_grch38_chain_path: str,
        min_r2: float,
    ) -> LDSet:
        """Create LDSet dataset for a specific population."""
        # Prepare LD Block matrix
        bm = BlockMatrix.read(ld_matrix_path)
        ldmatrix = LDSet._convert_ld_matrix_to_table(bm, min_r2)

        # Prepare table with variant indices
        ld_indices = LDSet._process_variant_indices(
            ld_index_path, grch37_to_grch38_chain_path
        ).persist()

        return LDSet(_df=ldmatrix)
