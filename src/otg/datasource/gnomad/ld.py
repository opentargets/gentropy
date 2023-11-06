"""Step to import filtered version of a LD matrix (block matrix)."""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from functools import reduce
from typing import TYPE_CHECKING

import hail as hl
import pyspark.sql.functions as f
from hail.linalg import BlockMatrix
from pyspark.sql import Window

from otg.common.utils import _liftover_loci, convert_gnomad_position_to_ensembl
from otg.dataset.ld_index import LDIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class GnomADLDMatrix:
    """Toolset ot interact with GnomAD LD dataset (version: r2.1.1).

    Datasets are accessed in Hail's native format, as provided by the [GnomAD consotiums](https://gnomad.broadinstitute.org/downloads/#v2-linkage-disequilibrium).

    Attributes:
        ld_matrix_template (str): Template for the LD matrix path. Defaults to "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm".
        ld_index_raw_template (str): Template for the LD index path. Defaults to "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht".
        grch37_to_grch38_chain_path (str): Path to the chain file used to lift over the coordinates. Defaults to "gs://hail-common/references/grch37_to_grch38.over.chain.gz".
        ld_populations (list[str]): List of populations to use to build the LDIndex. Defaults to ["afr", "amr", "asj", "eas", "fin", "nfe", "nwe", "seu"].
    """

    ld_matrix_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm"
    ld_index_raw_template: str = "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht"
    grch37_to_grch38_chain_path: str = (
        "gs://hail-common/references/grch37_to_grch38.over.chain.gz"
    )
    ld_populations: list[str] = field(
        defaultfactory=[
            "afr",  # African-American
            "amr",  # American Admixed/Latino
            "asj",  # Ashkenazi Jewish
            "eas",  # East Asian
            "fin",  # Finnish
            "nfe",  # Non-Finnish European
            "nwe",  # Northwestern European
            "seu",  # Southeastern European
        ]
    )

    @staticmethod
    def _aggregate_ld_index_across_populations(
        unaggregated_ld_index: DataFrame,
    ) -> DataFrame:
        """Aggregate LDIndex across populations.

        Args:
            unaggregated_ld_index (DataFrame): Unaggregate LDIndex index dataframe  each row is a variant pair in a population

        Returns:
            DataFrame: Aggregated LDIndex index dataframe  each row is a variant with the LD set across populations

        Examples:
            >>> data = [("1.0", "var1", "X", "var1", "pop1"), ("1.0", "X", "var2", "var2", "pop1"),
            ...         ("0.5", "var1", "X", "var2", "pop1"), ("0.5", "var1", "X", "var2", "pop2"),
            ...         ("0.5", "var2", "X", "var1", "pop1"), ("0.5", "X", "var2", "var1", "pop2")]
            >>> df = spark.createDataFrame(data, ["r", "variantId", "chromosome", "tagvariantId", "population"])
            >>> GnomADLDMatrix._aggregate_ld_index_across_populations(df).printSchema()
            root
             |-- variantId: string (nullable = true)
             |-- chromosome: string (nullable = true)
             |-- ldSet: array (nullable = false)
             |    |-- element: struct (containsNull = false)
             |    |    |-- tagVariantId: string (nullable = true)
             |    |    |-- rValues: array (nullable = false)
             |    |    |    |-- element: struct (containsNull = false)
             |    |    |    |    |-- population: string (nullable = true)
             |    |    |    |    |-- r: string (nullable = true)
            <BLANKLINE>
        """
        return (
            unaggregated_ld_index
            # First level of aggregation: get r/population for each variant/tagVariant pair
            .withColumn("r_pop_struct", f.struct("population", "r"))
            .groupBy("chromosome", "variantId", "tagVariantId")
            .agg(
                f.collect_set("r_pop_struct").alias("rValues"),
            )
            # Second level of aggregation: get r/population for each variant
            .withColumn("r_pop_tag_struct", f.struct("tagVariantId", "rValues"))
            .groupBy("variantId", "chromosome")
            .agg(
                f.collect_set("r_pop_tag_struct").alias("ldSet"),
            )
        )

    @staticmethod
    def _convert_ld_matrix_to_table(
        block_matrix: BlockMatrix, min_r2: float
    ) -> DataFrame:
        """Convert LD matrix to table.

        Args:
            block_matrix (BlockMatrix): LD matrix
            min_r2 (float): Minimum r2 value to keep in the table

        Returns:
            DataFrame: LD matrix as a Spark DataFrame
        """
        table = block_matrix.entries(keyed=False)
        return (
            table.filter(hl.abs(table.entry) >= min_r2**0.5)
            .to_spark()
            .withColumnRenamed("entry", "r")
        )

    @staticmethod
    def _create_ldindex_for_population(
        population_id: str,
        ld_matrix_path: str,
        ld_index_raw_path: str,
        grch37_to_grch38_chain_path: str,
        min_r2: float,
    ) -> DataFrame:
        """Create LDIndex for a specific population.

        Args:
            population_id (str): Population ID
            ld_matrix_path (str): Path to the LD matrix
            ld_index_raw_path (str): Path to the LD index
            grch37_to_grch38_chain_path (str): Path to the chain file used to lift over the coordinates
            min_r2 (float): Minimum r2 value to keep in the table

        Returns:
            DataFrame: LDIndex for a specific population
        """
        # Prepare LD Block matrix
        ld_matrix = GnomADLDMatrix._convert_ld_matrix_to_table(
            BlockMatrix.read(ld_matrix_path), min_r2
        )

        # Prepare table with variant indices
        ld_index = GnomADLDMatrix._process_variant_indices(
            hl.read_table(ld_index_raw_path),
            grch37_to_grch38_chain_path,
        )

        return GnomADLDMatrix._resolve_variant_indices(ld_index, ld_matrix).select(
            "*",
            f.lit(population_id).alias("population"),
        )

    @staticmethod
    def _process_variant_indices(
        ld_index_raw: hl.Table, grch37_to_grch38_chain_path: str
    ) -> DataFrame:
        """Creates a look up table between variants and their coordinates in the LD Matrix.

        !!! info "Gnomad's LD Matrix and Index are based on GRCh37 coordinates. This function will lift over the coordinates to GRCh38 to build the lookup table."

        Args:
            ld_index_raw (hl.Table): LD index table from GnomAD
            grch37_to_grch38_chain_path (str): Path to the chain file used to lift over the coordinates

        Returns:
            DataFrame: Look up table between variants in build hg38 and their coordinates in the LD Matrix
        """
        ld_index_38 = _liftover_loci(
            ld_index_raw, grch37_to_grch38_chain_path, "GRCh38"
        )

        return (
            ld_index_38.to_spark()
            # Filter out variants where the liftover failed
            .filter(f.col("`locus_GRCh38.position`").isNotNull())
            .withColumn(
                "chromosome", f.regexp_replace("`locus_GRCh38.contig`", "chr", "")
            )
            .withColumn(
                "position",
                convert_gnomad_position_to_ensembl(
                    f.col("`locus_GRCh38.position`"),
                    f.col("`alleles`").getItem(0),
                    f.col("`alleles`").getItem(1),
                ),
            )
            .select(
                "chromosome",
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("`alleles`").getItem(0),
                    f.col("`alleles`").getItem(1),
                ).alias("variantId"),
                f.col("idx"),
            )
            # Filter out ambiguous liftover results: multiple indices for the same variant
            .withColumn("count", f.count("*").over(Window.partitionBy(["variantId"])))
            .filter(f.col("count") == 1)
            .drop("count")
        )

    @staticmethod
    def _resolve_variant_indices(
        ld_index: DataFrame, ld_matrix: DataFrame
    ) -> DataFrame:
        """Resolve the `i` and `j` indices of the block matrix to variant IDs (build 38).

        Args:
            ld_index (DataFrame): Dataframe with resolved variant indices
            ld_matrix (DataFrame): Dataframe with the filtered LD matrix

        Returns:
            DataFrame: Dataframe with variant IDs instead of `i` and `j` indices
        """
        ld_index_i = ld_index.selectExpr(
            "idx as i", "variantId as variantId_i", "chromosome"
        )
        ld_index_j = ld_index.selectExpr("idx as j", "variantId as variantId_j")
        return (
            ld_matrix.join(ld_index_i, on="i", how="inner")
            .join(ld_index_j, on="j", how="inner")
            .drop("i", "j")
        )

    @staticmethod
    def _transpose_ld_matrix(ld_matrix: DataFrame) -> DataFrame:
        """Transpose LD matrix to a square matrix format.

        Args:
            ld_matrix (DataFrame): Triangular LD matrix converted to a Spark DataFrame

        Returns:
            DataFrame: Square LD matrix without diagonal duplicates

        Examples:
            >>> df = spark.createDataFrame(
            ...     [
            ...         (1, 1, 1.0, "1", "AFR"),
            ...         (1, 2, 0.5, "1", "AFR"),
            ...         (2, 2, 1.0, "1", "AFR"),
            ...     ],
            ...     ["variantId_i", "variantId_j", "r", "chromosome", "population"],
            ... )
            >>> GnomADLDMatrix._transpose_ld_matrix(df).show()
            +-----------+-----------+---+----------+----------+
            |variantId_i|variantId_j|  r|chromosome|population|
            +-----------+-----------+---+----------+----------+
            |          1|          2|0.5|         1|       AFR|
            |          1|          1|1.0|         1|       AFR|
            |          2|          1|0.5|         1|       AFR|
            |          2|          2|1.0|         1|       AFR|
            +-----------+-----------+---+----------+----------+
            <BLANKLINE>
        """
        ld_matrix_transposed = ld_matrix.selectExpr(
            "variantId_i as variantId_j",
            "variantId_j as variantId_i",
            "r",
            "chromosome",
            "population",
        )
        return ld_matrix.filter(
            f.col("variantId_i") != f.col("variantId_j")
        ).unionByName(ld_matrix_transposed)

    # @classmethod
    def as_ld_index(
        self: GnomADLDMatrix,
        min_r2: float,
    ) -> LDIndex:
        """Create LDIndex dataset aggregating the LD information across a set of populations.

        **The basic steps to generate the LDIndex are:**

        1. Convert LD matrix to a Spark DataFrame.
        2. Resolve the matrix indices to variant IDs by lifting over the coordinates to GRCh38.
        3. Aggregate the LD information across populations.

        Args:
            min_r2 (float): Minimum r2 value to keep in the table

        Returns:
            LDIndex: LDIndex dataset
        """
        ld_indices_unaggregated = []
        for pop in self.ld_populations:
            try:
                ld_matrix_path = self.ld_matrix_template.format(POP=pop)
                ld_index_raw_path = self.ld_index_raw_template.format(POP=pop)
                pop_ld_index = self._create_ldindex_for_population(
                    pop,
                    ld_matrix_path,
                    ld_index_raw_path.format(pop),
                    self.grch37_to_grch38_chain_path,
                    min_r2,
                )
                ld_indices_unaggregated.append(pop_ld_index)
            except Exception as e:
                print(f"Failed to create LDIndex for population {pop}: {e}")
                sys.exit(1)

        ld_index_unaggregated = (
            GnomADLDMatrix._transpose_ld_matrix(
                reduce(lambda df1, df2: df1.unionByName(df2), ld_indices_unaggregated)
            )
            .withColumnRenamed("variantId_i", "variantId")
            .withColumnRenamed("variantId_j", "tagVariantId")
        )
        return LDIndex(
            _df=self._aggregate_ld_index_across_populations(ld_index_unaggregated),
            _schema=LDIndex.get_schema(),
        )
