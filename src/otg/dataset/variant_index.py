"""Variant index dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Type

import pyspark.sql.functions as f
from omegaconf import MISSING

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import nullify_empty_array
from otg.dataset.dataset import Dataset
from otg.dataset.variant_annotation import VariantAnnotation

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


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
    def from_parquet(
        cls: Type[VariantIndex], etl: ETLSession, path: str
    ) -> VariantIndex:
        """Initialise VariantIndex from parquet file.

        Args:
            etl (ETLSession): ETL session
            path (str): Path to parquet file

        Returns:
            VariantIndex: VariantIndex dataset
        """
        return super(Dataset, cls).from_parquet(etl, path, cls.schema)

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

        # Reduce scope of variant annotation dataset to only variants in credible sets:
        va_slimmed = va.filter_by_variant_df(credset, ["id", "chromosome"])

        return VariantIndex.from_variant_annotation(
            variant_annotation=va_slimmed, path=path
        )

    @classmethod
    def from_variant_annotation(
        cls: type[VariantIndex],
        variant_annotation: VariantAnnotation,
        path: Optional[str] = None,
    ) -> VariantIndex:
        """Initialise VariantIndex from pre-existing variant annotation dataset."""
        unchanged_cols = [
            "id",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "chromosomeB37",
            "positionB37",
            "alleleType",
            "alleleFrequencies",
            "cadd",
        ]
        vi = cls(
            path=path,
            df=variant_annotation.df.select(
                *unchanged_cols,
                f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
                # filters/rsid are arrays that can be empty, in this case we convert them to null
                nullify_empty_array(f.col("filters")).alias("filters"),
                nullify_empty_array(f.col("rsIds")).alias("rsIds"),
                f.lit(True).alias("variantInGnomad"),
            ),
        )
        vi.validate_schema()
        return vi.df.repartition(
            400,
            "chromosome",
        ).sortWithinPartitions("chromosome", "position")


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
