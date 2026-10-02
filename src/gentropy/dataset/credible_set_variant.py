"""Credible set variant dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class CredibleSetVariant(Dataset):
    """Credible set variant dataset.

    One row per variant per `CredibleSet`. `conditionalEffect` carries the effect
    estimated by the fine-mapper, and `rescaledEffect` the effect derived from it.
    Both credible interval flags are stored.

    Examples:
        >>> data = [("cs1", "1_154453788_C_T", True, True, 5.2, 0.81, (1, 0.1, 0.015), None, [])]
        >>> credible_set_variant = CredibleSetVariant(_df=spark.createDataFrame(data, CredibleSetVariant.get_schema()))
        >>> credible_set_variant.df.show(truncate=False)
        +-------------+---------------+---------------+---------------+----------------+--------------------+-----------------+--------------+---------------+
        |credibleSetId|variantId      |is95CredibleSet|is99CredibleSet|log10BayesFactor|posteriorProbability|conditionalEffect|rescaledEffect|qualityControls|
        +-------------+---------------+---------------+---------------+----------------+--------------------+-----------------+--------------+---------------+
        |cs1          |1_154453788_C_T|true           |true           |5.2             |0.81                |{1, 0.1, 0.015}  |NULL          |[]             |
        +-------------+---------------+---------------+---------------+----------------+--------------------+-----------------+--------------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[CredibleSetVariant]) -> StructType:
        """Provides the schema for the CredibleSetVariant dataset.

        Returns:
            StructType: Schema for the CredibleSetVariant dataset
        """
        return parse_spark_schema("credible_set_variant.json")
