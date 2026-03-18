"""Variant direction dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pyspark.sql.functions as f
from pyspark.sql import Column
from pyspark.sql import types as t

from gentropy import VariantIndex
from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset


class Direction(int, Enum):
    """Allele direction.

    Attributes:
        DIRECT (int): Direct allele direction (e.g., A/G). Defaults to 1.
        FLIPPED (int): Flipped allele direction (e.g., G/A). Defaults to -1.
    """

    DIRECT = 1
    FLIPPED = -1


class Strand(int, Enum):
    """Strand orientation.

    Attributes:
        FORWARD (int): Forward strand. Defaults to 1.
        REVERSE (int): Reverse strand. Defaults to -1.
    """

    FORWARD = 1
    REVERSE = -1


class VariantType(int, Enum):
    """Variant types based on length of reference and alternate alleles.

    Attributes:
        SNP (int): Single Nucleotide Polymorphism. Defaults to 1.
        INS (int): Insertion. Defaults to 2.
        DEL (int): Deletion. Defaults to 3.
        MNP (int): Multi-Nucleotide Polymorphism. Defaults to 4.
    """

    SNP = 1
    INS = 2
    DEL = 3
    MNP = 4


@dataclass
class VariantDirection(Dataset):
    """Dataset used for aligning allele directionality between different datasets.

    This dataset is useful for flipping alleles to match reference datasets.

    This dataset expends each variant into 4 entries to account for

    * Different directions (`FORWARD` and `FLIPPED`) - e.g. A/G and G/A
    * Different strands (`FORWARD` and `REVERSE`) - e.g. A/G and T/C

    Each entry contains the combination of both, meaning that for each input variant
    there will be 4 entries in this dataset. For strand ambiguous variants the
    `FORWARD` and `FLIPPED` entries will be identical to the `REVERSE` and `FLIPPED` entries,
    so we keep only one copy of them.

    Additionally this dataset annotates:

    * ambiguous strand variants
    * type of variant (SNP, INS, DEL, MNP)
    * original allele frequencies from the source dataset
    * original variant id from the source dataset

    ??? tip "Joining with other datasets"
        To compare two datasets, you need to ensure that both datasets are joined on the `variantId` that
        is a combination of `chromosome`, `position`, `reference allele` and `alternate allele`.

    ??? note "Building the dataset"
        The easiest way to create this dataset (have a complete variant space) is to build it
        from a **VariantIndex**.

    Examples:
        >>> data = [("1", 100, "A", "G", [("nfe_adj", 0.1), ("fin_adj", 0.2)]), ("1", 100, "T", "A", [("nfe_adj", 0.1), ("fin_adj", 0.2)])]
        >>> schema = "chromosome STRING, position INT, referenceAllele STRING, alternateAllele STRING, alleleFrequencies ARRAY<STRUCT<populationName: STRING, alleleFrequency: DOUBLE>>"
        >>> df = spark.createDataFrame(data, schema).withColumn("variantId",
        ... f.concat_ws("_", "chromosome", "position", "referenceAllele", "alternateAllele"))
        >>> variant_index = VariantIndex(_df=df)
        >>> variant_direction = VariantDirection.from_variant_index(variant_index)
        >>> variant_direction.df.show(truncate=False)
        +----------+-----------------+----+---------+---------+------+-----------------+--------------------------------+
        |chromosome|originalVariantId|type|variantId|direction|strand|isStrandAmbiguous|originalAlleleFrequencies       |
        +----------+-----------------+----+---------+---------+------+-----------------+--------------------------------+
        |1         |1_100_A_G        |1   |1_100_A_G|1        |1     |false            |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        |1         |1_100_A_G        |1   |1_100_G_A|-1       |1     |false            |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        |1         |1_100_A_G        |1   |1_100_T_C|1        |-1    |false            |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        |1         |1_100_A_G        |1   |1_100_C_T|-1       |-1    |false            |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        |1         |1_100_T_A        |1   |1_100_T_A|1        |1     |true             |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        |1         |1_100_T_A        |1   |1_100_A_T|-1       |1     |true             |[{nfe_adj, 0.1}, {fin_adj, 0.2}]|
        +----------+-----------------+----+---------+---------+------+-----------------+--------------------------------+
        <BLANKLINE>

    """

    @classmethod
    def get_schema(cls: type[VariantDirection]) -> t.StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            t.StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_direction.json")

    @classmethod
    def is_strand_ambiguous(cls, ref: Column, alt: Column) -> Column:
        """Check if the variant is strand ambiguous.

        Args:
            ref (Column): Reference allele column.
            alt (Column): Alternate allele column.

        Returns:
            Column: Boolean column indicating if the variant is palindromic.

        Examples:
            >>> data = [("A", "T"), ("C", "G"), ("A", "G"), ("AC", "GT"), ("AT", "TA"), ("A", "AT")]
            >>> schema = "ref STRING, alt STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("isStrandAmbiguous", VariantDirection.is_strand_ambiguous(f.col("ref"), f.col("alt"))).show()
            +---+---+-----------------+
            |ref|alt|isStrandAmbiguous|
            +---+---+-----------------+
            |  A|  T|             true|
            |  C|  G|             true|
            |  A|  G|            false|
            | AC| GT|             true|
            | AT| TA|            false|
            |  A| AT|            false|
            +---+---+-----------------+
            <BLANKLINE>

        """
        ref_len = f.length(ref)
        alt_len = f.length(alt)
        ref = f.upper(ref)
        alt = f.upper(alt)
        return f.when(
            (ref_len == alt_len) & (cls.reverse(cls.complement(alt)) == ref), True
        ).otherwise(False)

    @staticmethod
    def reverse(allele: Column) -> Column:
        """Reverse the allele string.

        Args:
            allele (Column): Allele column.

        Returns:
            Column: Reversed allele column.

        Examples:
            >>> data = [("A",), ("AT",), ("GTC",)]
            >>> schema = "allele STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("reversed", VariantDirection.reverse(f.col("allele"))).show()
            +------+--------+
            |allele|reversed|
            +------+--------+
            |     A|       A|
            |    AT|      TA|
            |   GTC|     CTG|
            +------+--------+
            <BLANKLINE>

        """
        return f.reverse(allele)

    @staticmethod
    def complement(allele: Column) -> Column:
        """Complement the allele string.

        Args:
            allele (Column): Allele column.

        Returns:
            Column: Complemented allele column.

        Examples:
            >>> data = [("A",), ("C",), ("G",), ("T",), ("AT",), ("GTC",)]
            >>> schema = "allele STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("complemented", VariantDirection.complement(f.col("allele"))).show()
            +------+------------+
            |allele|complemented|
            +------+------------+
            |     A|           T|
            |     C|           G|
            |     G|           C|
            |     T|           A|
            |    AT|          TA|
            |   GTC|         CAG|
            +------+------------+
            <BLANKLINE>

        """
        return f.translate(f.upper(allele), "ACGT", "TGCA")

    @classmethod
    def variant_type(cls, ref: Column, alt: Column) -> Column:
        """Get the variant type.

        Args:
            ref (Column): Reference allele column.
            alt (Column): Alternate allele column.

        Returns:
            Column: Variant type column.

        Note:
            Variant type coding follows VariantType enum:
            - 1: SNP (Single Nucleotide Polymorphism)
            - 2: INS (Insertion)
            - 3: DEL (Deletion)
            - 4: MNP (Multi-Nucleotide Polymorphism)

        Examples:
            >>> data = [("A", "G"), ("A", "AT"), ("AT", "A"), ("AT", "GC")]
            >>> schema = "ref STRING, alt STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("type", VariantDirection.variant_type(f.col("ref"), f.col("alt"))).show()
            +---+---+----+
            |ref|alt|type|
            +---+---+----+
            |  A|  G|   1|
            |  A| AT|   2|
            | AT|  A|   3|
            | AT| GC|   4|
            +---+---+----+
            <BLANKLINE>
        """
        ref = f.upper(ref)
        alt = f.upper(alt)
        expr = (
            f.when((f.length(alt) > f.length(ref)), f.lit(VariantType.INS.value))
            .when((f.length(alt) < f.length(ref)), f.lit(VariantType.DEL.value))
            .when(
                ((f.length(alt) == 1) & (f.length(ref) == 1)),
                f.lit(VariantType.SNP.value),
            )
            .otherwise(f.lit(VariantType.MNP.value))
        )
        return expr.cast(t.ByteType())

    @classmethod
    def alleles(
        cls, chrom: Column, pos: Column, ref: Column, alt: Column, af: Column
    ) -> Column:
        """Get the alleles of the variant.

        Args:
            chrom (Column): Chromosome column.
            pos (Column): Position column.
            ref (Column): Reference allele column.
            alt (Column): Alternate allele column.
            af (Column): Allele frequencies column.

        Returns:
            Column: Array of structs with variantId, direction, strand, isStrandAmbiguous, alleleFrequencies.

        Examples:
            >>> data = [("1", 100, "A", "G", [("nfe_adj", 0.1),]), ("1", 100, "T", "A", [("nfe_adj", 0.1),])]
            >>> schema = "chrom STRING, pos INT, ref STRING, alt STRING, af ARRAY<STRUCT<populationName: STRING, alleleFrequency: DOUBLE>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("alleles", VariantDirection.alleles(f.col("chrom"), f.col("pos"), f.col("ref"), f.col("alt"), f.col("af"))).select("alleles")
            >>> df.select(f.explode("alleles").alias("allele")).select("allele.*").show(truncate=False)
            +---------+---------+------+-----------------+-------------------------+
            |variantId|direction|strand|isStrandAmbiguous|originalAlleleFrequencies|
            +---------+---------+------+-----------------+-------------------------+
            |1_100_A_G|1        |1     |false            |[{nfe_adj, 0.1}]         |
            |1_100_G_A|-1       |1     |false            |[{nfe_adj, 0.1}]         |
            |1_100_T_C|1        |-1    |false            |[{nfe_adj, 0.1}]         |
            |1_100_C_T|-1       |-1    |false            |[{nfe_adj, 0.1}]         |
            |1_100_T_A|1        |1     |true             |[{nfe_adj, 0.1}]         |
            |1_100_A_T|-1       |1     |true             |[{nfe_adj, 0.1}]         |
            +---------+---------+------+-----------------+-------------------------+
            <BLANKLINE>
        """
        ref = f.upper(ref)
        alt = f.upper(alt)
        forward_direct = cls.variant_id(chrom, pos, ref, alt)
        forward_flipped = cls.variant_id(chrom, pos, alt, ref)
        reverse_direct = cls.variant_id(
            chrom,
            pos,
            cls.reverse(cls.complement(ref)),
            cls.reverse(cls.complement(alt)),
        )
        reverse_flipped = cls.variant_id(
            chrom,
            pos,
            cls.reverse(cls.complement(alt)),
            cls.reverse(cls.complement(ref)),
        )

        return f.when(
            ~cls.is_strand_ambiguous(ref, alt),
            f.array(
                f.struct(
                    forward_direct.alias("variantId"),
                    f.lit(Direction.DIRECT.value).cast(t.ByteType()).alias("direction"),
                    f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                    f.lit(False).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
                f.struct(
                    forward_flipped.alias("variantId"),
                    f.lit(Direction.FLIPPED.value)
                    .cast(t.ByteType())
                    .alias("direction"),
                    f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                    f.lit(False).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
                f.struct(
                    reverse_direct.alias("variantId"),
                    f.lit(Direction.DIRECT.value).cast(t.ByteType()).alias("direction"),
                    f.lit(Strand.REVERSE.value).cast(t.ByteType()).alias("strand"),
                    f.lit(False).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
                f.struct(
                    reverse_flipped.alias("variantId"),
                    f.lit(Direction.FLIPPED.value)
                    .cast(t.ByteType())
                    .alias("direction"),
                    f.lit(Strand.REVERSE.value).cast(t.ByteType()).alias("strand"),
                    f.lit(False).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
            ),
        ).otherwise(
            f.array(
                f.struct(
                    forward_direct.alias("variantId"),
                    f.lit(Direction.DIRECT.value).cast(t.ByteType()).alias("direction"),
                    f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                    f.lit(True).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
                f.struct(
                    forward_flipped.alias("variantId"),
                    f.lit(Direction.FLIPPED.value)
                    .cast(t.ByteType())
                    .alias("direction"),
                    f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                    f.lit(True).alias("isStrandAmbiguous"),
                    af.alias("originalAlleleFrequencies"),
                ),
            )
        )

    @classmethod
    def variant_id(cls, chrom: Column, pos: Column, ref: Column, alt: Column) -> Column:
        """Get the variant id.

        Args:
            chrom (Column): Chromosome column.
            pos (Column): Position column.
            ref (Column): Reference allele column.
            alt (Column): Alternate allele column.

        Returns:
            Column: Variant ID column in the format "chrom_pos_ref_alt".
        """
        ref = f.upper(ref)
        alt = f.upper(alt)
        return f.concat_ws("_", chrom, pos, ref, alt)

    @classmethod
    def from_variant_index(cls, variant_index: VariantIndex) -> VariantDirection:
        """Prepare the variant direction DataFrame with DIRECT and FLIPPED entries.

        Args:
            variant_index (VariantIndex): Variant index dataset.

        Returns:
            VariantDirection: Variant direction dataset.
        """
        lut = variant_index.df.select(
            f.col("chromosome"),
            f.col("variantId").alias("originalVariantId"),
            cls.variant_type(f.col("referenceAllele"), f.col("alternateAllele")).alias(
                "type"
            ),
            f.explode(
                cls.alleles(
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
                    f.col("alleleFrequencies"),
                ).alias("alleles")
            ).alias("allele"),
        ).select(
            f.col("chromosome"),
            f.col("originalVariantId"),
            f.col("type"),
            f.col("allele.variantId").alias("variantId"),
            f.col("allele.direction").alias("direction"),
            f.col("allele.strand").alias("strand"),
            f.col("allele.isStrandAmbiguous").alias("isStrandAmbiguous"),
            f.col("allele.originalAlleleFrequencies").alias(
                "originalAlleleFrequencies"
            ),
        )

        return VariantDirection(_df=lut)
