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
        """Check if the variant is palindromic."""
        ref_len = f.length(ref)
        alt_len = f.length(alt)
        return f.when(
            (ref_len == alt_len) & (cls.reverse(cls.complement(ref)) == alt), True
        ).otherwise(False)

    @staticmethod
    def reverse(allele: Column) -> Column:
        """Reverse the allele string."""
        return f.reverse(allele)

    @staticmethod
    def complement(allele: Column) -> Column:
        """Complement the allele string."""
        return f.translate(f.upper(allele), "ACGT", "TGCA")

    @classmethod
    def variant_type(cls, ref: Column, alt: Column) -> Column:
        """Get the variant type."""
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
    def get_variant_len(cls, ref: Column, alt: Column) -> Column:
        """Get the indel length of the variant.

        Note:
        ----
        Effective length is defined as the absolute difference between the lengths of the reference and alternate alleles.

        """
        return f.abs(f.length(alt) - f.length(ref))

    @classmethod
    def get_variant_end(cls, pos: Column, ref: Column, alt: Column) -> Column:
        """Get the end position of the variant."""
        return pos + cls.get_variant_len(ref, alt).cast(t.IntegerType())

    @classmethod
    def alleles(
        cls, chrom: Column, pos: Column, ref: Column, alt: Column, af: Column
    ) -> Column:
        """Get the alleles of the variant."""
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

    # def flip_sumstats(self, sumstats: SummaryStatistics) -> sumstats:
    #     """Flip beta in summary statistics when they map to FLIPPED or keep the DIRECT beta"""
    #     va = self.prepare().
    #     merged = sumstats.join(
    #         va,
    #         on=(
    #             (va.chromosome=sumstats.chromosome) &
    #             (va.variantId = sumstats.variantId)
    #         ),
    #         how="inner"
    #     )
    #     return SummaryStatistics(
    #         _df=merged.withColumn(
    #             "beta",
    #             f.when(f.col("direction") == Direction.FLIPPED, -1 * f.col("beta")).otherwise(f.col("beta"))
    #         )
    #     )
