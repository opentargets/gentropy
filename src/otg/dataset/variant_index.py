"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

import pyspark.sql.functions as f
from omegaconf import MISSING

from otg.common.schemas import parse_spark_schema
from otg.data.dataset import Dataset
from otg.data.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common import ETLSession


@dataclass
class VariantIndexCredsetConfig:
    """Variant index from credible set configuration."""

    path: Optional[str] = None
    variant_annotation: Any = MISSING
    credible_sets: str = MISSING


@dataclass
class VariantIndex(Dataset):
    """Variant index dataset.

    Variant index dataset is the result of intersecting the variant annotation (gnomad) dataset with the variants with V2D available information.
    """

    schema: StructType = parse_spark_schema("variant_index.json")

    @classmethod
    def from_credset(
        cls: type[VariantIndex],
        etl: ETLSession,
        variant_annotation_path: str,
        credset_path: str,
        path: Optional[str] = None,
    ) -> VariantIndex:
        """Initialise VariantIndex.

        Args:
            etl (ETLSession): ETL session
            variant_annotation_path (str): Path to variant annotation dataset
            credset_path (str): Path to credible set dataset
            path (str, optional): Path to save the dataset. Defaults to None.

        Returns:
            VariantIndex: Variant index dataset
        """
        # Read variant annotation dataset:
        va = VariantAnnotation.from_parquet(etl, variant_annotation_path)

        # Extract variants from credible sets:
        # TODO: implement this as a method of incoming study-locus dataset
        credset = get_variants_from_credset(etl, credset_path)

        df = (
            credset.join(
                va.df.join(f.broadcast(credset), on=["id", "chromosome"], how="inner"),
                on=["id", "chromosome"],
                how="left",
            )
            .withColumn(
                "variantInGnomad", f.coalesce(f.col("variantInGnomad"), f.lit(False))
            )
            .repartition(
                400,
                "chromosome",
            )
            .sortWithinPartitions("chromosome", "position")
            .persist()
        )
        return cls(
            path=path,
            df=df.filter(f.col("variantInGnomad")).drop("variantInGnomad"),
            invalid_variants=df.filter(~f.col("variantInGnomad")).select("id"),
        )


# TODO: Implement this as a method of incoming study-locus dataset
def get_variants_from_credset(etl: ETLSession, credible_sets: str) -> DataFrame:
    """It reads the credible sets from the given path, extracts the lead and tag variants.

    Args:
        etl (ETLSession): ETL session
        credible_sets (str): Path to the credible sets

    Returns:
        DataFrame: A dataframe with all variants contained in the credible sets
    """
    credset = (
        etl.spark.read.parquet(credible_sets)
        .select(
            "leadVariantId",
            "tagVariantId",
            f.split(f.col("leadVariantId"), "_")[0].alias("chromosome"),
        )
        .repartition("chromosome")
        .persist()
    )
    return (
        credset.selectExpr("leadVariantId as id", "chromosome")
        .union(credset.selectExpr("tagVariantId as id", "chromosome"))
        .dropDuplicates(["id"])
    )
