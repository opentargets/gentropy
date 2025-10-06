"""Variant direction dataset."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
from pyspark.sql import types as t

from gentropy import Session, VariantIndex
from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset


@dataclass
class VariantDirection(Dataset):
    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_index.json")


from enum import Enum
from gentropy.dataset.summary_statistics import SummaryStatistics


class Direction(str, Enum):
    DIRECT = "direct"
    FLIPPED = "flipped"
    

class VariantFlipper:
    def __init__(self, vi: VariantIndex) -> None:
        self.vi = vi
    
    @staticmethod
    def variant_id(chrom: Column, pos: Column, ref: Column, alt: Column) -> Column:
        """Get the variant id"""
        return f.concat_ws("_", chrom, pos, ref, alt)


    def prepare(self) -> DataFrame:
        direct =  self.vi.df.select(
            f.col("chromosome"), 
            self.variant_id(
                f.col("chromosome"), 
                f.col("position"),
                f.col("referenceAllele"),
                f.col("alternateAllele"),
            ).alias("variantId"),
            f.lit(Direction.DIRECT).alias("direction")
        )
        flipped = self.vi.df.select(
            f.col("chromosome"), 
            self.variant_id(
                f.col("chromosome"), 
                f.col("position"),
                f.col("alternateAllele"),
                f.col("referenceAllele"),
            ).alias("variantId"),
            f.lit(Direction.FLIPPED).alias("direction")

        )
        return direct.unionByName(flipped)
    
    def flip_sumstats(self, sumstats: SummaryStatistics) -> sumstats:
        """Flip beta in summary statistics when they map to FLIPPED or keep the DIRECT beta"""
        va = self.prepare().
        merged = sumstats.join(
            va, 
            on=(
                (va.chromosome=sumstats.chromosome) & 
                (va.variantId = sumstats.variantId)
            ), 
            how="inner"
        )
        return SummaryStatistics(
            _df=merged.withColumn(
                "beta", 
                f.when(f.col("direction") == Direction.FLIPPED, -1 * f.col("beta")).otherwise(f.col("beta"))
            )
        )