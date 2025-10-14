"""Variant direction dataset."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pyspark.sql.functions as f
from pyspark.sql import Column
from pyspark.sql import types as t

from gentropy import VariantIndex
from gentropy.dataset.dataset import Dataset


class Direction(int, Enum):
    """Allele direction."""

    DIRECT = 1
    FLIPPED = -1


class Strand(int, Enum):
    """Strand orientation."""

    FORWARD = 1
    REVERSE = -1


class VariantType(int, Enum):
    """Variant types based on length of reference and alternate alleles."""

    SNP = 1
    INS = 2
    DEL = 3
    MNP = 4  # multi-variant polymorphism


@dataclass
class VariantDirection(Dataset):
    """Variant direction dataset."""

    @classmethod
    def get_schema(cls: type[VariantDirection]) -> t.StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return t.StructType(
            [
                t.StructField("chromosome", t.StringType(), nullable=False),
                t.StructField("originalVariantId", t.StringType(), nullable=False),
                t.StructField("type", t.ByteType(), nullable=True),
                t.StructField("variantId", t.StringType(), nullable=False),
                t.StructField("direction", t.ByteType(), nullable=False),
                t.StructField("strand", t.ByteType(), nullable=True),
                t.StructField("isPalindromic", t.BooleanType(), nullable=True),
                t.StructField(
                    "alleleFrequencies",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField(
                                    "populationName", t.StringType(), nullable=False
                                ),
                                t.StructField(
                                    "alleleFrequency", t.DoubleType(), nullable=False
                                ),
                            ]
                        )
                    ),
                    nullable=True,
                ),
            ]
        )

    @classmethod
    def is_palindromic(cls, ref: Column, alt: Column) -> Column:
        """Check if the variant is palindromic.

        Args:
            ref (Column): Reference allele column.
            alt (Column): Alternate allele column.

        Returns:
            Column: Boolean column indicating if the variant is palindromic.

        Examples:
            >>> data = [("A", "T"), ("C", "G"), ("A", "G"), ("AT", "TA"), ("A", "AT")]
            >>> schema = "ref STRING, alt STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("isPalindromic", VariantDirection.is_palindromic(f.col("ref"), f.col("alt"))).show()
            +---+---+-------------+
            |ref|alt|isPalindromic|
            +---+---+-------------+
            |  A|  T|         true|
            |  C|  G|         true|
            |  A|  G|        false|
            | AT| TA|         true|
            |  A| AT|        false|
            +---+---+-------------+
            <BLANKLINE>

        """
        ref_len = f.length(ref)
        alt_len = f.length(alt)
        return f.when(
            (ref_len == alt_len) & (cls.reverse(cls.complement(ref)) == alt), True
        ).otherwise(False)

    @staticmethod
    def reverse(allele: Column) -> Column:
        """Reverse the allele string.

        Args:
            allele (Column): Allele column.

        Returns:
            Column: Reversed allele column.

        Examples:
            >>> data = [("A"), ("AT"), ("GTC")]
            >>> schema = "allele STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("reversed", VariantDirection.reverse(f.col("allele"))).show()
            +-------+--------+
            | allele|reversed|
            +-------+--------+
            |      A|       A|
            |     AT|      TA|
            |    GTC|     CTG|
            +-------+--------+
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
            >>> data = [("A"), ("C"), ("G"), ("T"), ("AT"), ("GTC")]
            >>> schema = "allele STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.withColumn("complemented", VariantDirection.complement(f.col("allele"))).show()
            +-------+------------+
            | allele|complemented|
            +-------+------------+
            |      A|           T|
            |      C|           G|
            |      G|           C|
            |      T|           A|
            |     AT|          TA|
            |    GTC|         CAG|
            +-------+------------+
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
            Column: Array of structs with variantId, direction, strand, isPalindromic, alleleFrequencies.

        Examples:
            >>> data = [("1", 100, "A", "G", [("nfe_adj", 0.1),])]
            >>> schema = "chrom STRING, pos INT, ref STRING, alt STRING, af ARRAY<STRUCT<populationName: STRING, alleleFrequency: DOUBLE>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("alleles", VariantDirection.alleles("chrom", "pos", "ref", "alt", "af")).select("alleles")
            >>> df.select(f.explode("alleles").alias("allele")).select("allele.*").show(truncate=False)
            +----------+---------+------+-------------+-----------------+
            |variantId |direction|strand|isPalindromic|alleleFrequencies|
            +----------+---------+------+-------------+-----------------+
            |1_100_A_G |1        |1     |false        |[{nfe_adj, 0.1}] |
            |1_100_G_A |-1       |1     |false        |[{nfe_adj, 0.9}] |
            |1_100_T_C |1        |-1    |false        |[{nfe_adj, 0.1}] |
            |1_100_C_T |-1       |-1    |false        |[{nfe_adj, 0.9}] |
            +----------+---------+------+-------------+-----------------+
            <BLANKLINE>
        """
        forward_direct = cls.variant_id(chrom, pos, ref, alt)  # A/G
        forward_flipped = cls.variant_id(chrom, pos, alt, ref)  # G/A
        reverse_direct = cls.variant_id(  # T/C
            chrom,
            pos,
            cls.reverse(cls.complement(ref)),
            cls.reverse(cls.complement(alt)),
        )
        reverse_flipped = cls.variant_id(  # C/T
            chrom,
            pos,
            cls.reverse(cls.complement(alt)),
            cls.reverse(cls.complement(ref)),
        )

        return f.array(
            f.struct(
                forward_direct.alias("variantId"),
                f.lit(Direction.DIRECT.value).cast(t.ByteType()).alias("direction"),
                f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                cls.is_palindromic(ref, alt).alias("isPalindromic"),
                af.alias("alleleFrequencies"),
            ),
            f.struct(
                forward_flipped.alias("variantId"),
                f.lit(Direction.FLIPPED.value).cast(t.ByteType()).alias("direction"),
                f.lit(Strand.FORWARD.value).cast(t.ByteType()).alias("strand"),
                cls.is_palindromic(ref, alt).alias("isPalindromic"),
                f.transform(
                    af,
                    lambda x: f.struct(
                        x["populationName"],
                        (1.0 - x["alleleFrequency"]).alias("alleleFrequency"),
                    ),
                ).alias("alleleFrequencies"),
            ),
            f.struct(
                reverse_direct.alias("variantId"),
                f.lit(Direction.DIRECT.value).cast(t.ByteType()).alias("direction"),
                f.lit(Strand.REVERSE.value).cast(t.ByteType()).alias("strand"),
                cls.is_palindromic(ref, alt).alias("isPalindromic"),
                af.alias("alleleFrequencies"),
            ),
            f.struct(
                reverse_flipped.alias("variantId"),
                f.lit(Direction.FLIPPED.value).cast(t.ByteType()).alias("direction"),
                f.lit(Strand.REVERSE.value).cast(t.ByteType()).alias("strand"),
                cls.is_palindromic(ref, alt).alias("isPalindromic"),
                f.transform(
                    af,
                    lambda x: f.struct(
                        x["populationName"],
                        (1.0 - x["alleleFrequency"]).alias("alleleFrequency"),
                    ),
                ).alias("alleleFrequencies"),
            ),
        )

    @classmethod
    def normalize_chromosome(cls, chrom: Column) -> Column:
        """Normalize chromosome names.

        This includes:
        - Removing "chr" prefix
        - Converting 23 to X, 24 to Y, and M to MT

        Args:
            chrom (Column): Chromosome column

        Returns:
            Column: Normalized chromosome column

        """
        ensembl_chr = f.regexp_replace(chrom.cast(t.StringType()), "^chr", "")

        return (
            f.when(ensembl_chr == f.lit("23"), f.lit("X"))
            .when(ensembl_chr == f.lit("24"), f.lit("Y"))
            .when(ensembl_chr == f.lit("M"), f.lit("MT"))
            .otherwise(ensembl_chr)
        )

    @classmethod
    def variant_id(cls, chrom: Column, pos: Column, ref: Column, alt: Column) -> Column:
        """Get the variant id."""
        return f.concat_ws("_", cls.normalize_chromosome(chrom), pos, ref, alt)

    @classmethod
    def from_variant_index(cls, variant_index: VariantIndex) -> VariantDirection:
        """Prepare the variant direction DataFrame with DIRECT and FLIPPED entries."""
        lut = variant_index.df.select(
            cls.normalize_chromosome(f.col("chromosome")).alias("chromosome"),
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
            f.col("allele.isPalindromic").alias("isPalindromic"),
            f.col("allele.alleleFrequencies").alias("alleleFrequencies"),
        )

        return VariantDirection(_df=lut)
