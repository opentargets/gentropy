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
        >>> data = [
        ...     ("cs1", "1_154453788_C_T", True, True, 5.2, 0.7, (1, 0.1, 0.015), None, []),
        ...     ("cs1", "1_154460000_G_A", True, True, 4.8, 0.3, (1, 0.04, 0.012), None, []),
        ...     ("cs2", "1_154600000_A_C", True, True, 3.9, 0.6, (-1, 0.08, 0.017), None, []),
        ...     ("cs2", "1_154610000_C_T", True, True, 3.7, 0.4, (-1, 0.05, 0.014), None, []),
        ... ]
        >>> credible_set_variant = CredibleSetVariant(_df=spark.createDataFrame(data, CredibleSetVariant.get_schema()))
        >>> credible_set_variant.df.show(truncate=False)
        +-------------+---------------+---------------+---------------+----------------+--------------------+-----------------+--------------+---------------+
        |credibleSetId|variantId      |is95CredibleSet|is99CredibleSet|log10BayesFactor|posteriorProbability|conditionalEffect|rescaledEffect|qualityControls|
        +-------------+---------------+---------------+---------------+----------------+--------------------+-----------------+--------------+---------------+
        |cs1          |1_154453788_C_T|true           |true           |5.2             |0.7                 |{1, 0.1, 0.015}  |NULL          |[]             |
        |cs1          |1_154460000_G_A|true           |true           |4.8             |0.3                 |{1, 0.04, 0.012} |NULL          |[]             |
        |cs2          |1_154600000_A_C|true           |true           |3.9             |0.6                 |{-1, 0.08, 0.017}|NULL          |[]             |
        |cs2          |1_154610000_C_T|true           |true           |3.7             |0.4                 |{-1, 0.05, 0.014}|NULL          |[]             |
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
