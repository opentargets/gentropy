"""LDIndex dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class LDIndex(Dataset):
    """Pairs of variants in linkage disequilibrium, one row per pair and population.

    Each row gives the correlation ``r`` between ``variantId`` and ``tagVariantId``
    in one reference ``population``, signed for the alternate alleles of the two
    variant IDs, except for FinnGen (``fin``), whose sign is for the minor alleles
    (see ``FinnGenLDMatrix``): only ``r`` squared is meaningful there. Both orders
    of a pair are stored, so a variant's tags are the rows with its ``variantId``,
    and every variant of the panel tags itself at ``r`` 1: a variant with no row
    is not in the panel.

    The index is stored partitioned by ``population`` and ``chromosome``, so that
    each population is rebuilt on its own and read without the others.

    Examples:
        >>> df = spark.createDataFrame(
        ...     [("nfe", "1", "1_100_A_T", "1_100_A_T", 1.0),
        ...      ("nfe", "1", "1_100_A_T", "1_200_C_G", -0.8)],
        ...     LDIndex.get_schema(),
        ... )
        >>> LDIndex(_df=df).df.columns
        ['population', 'chromosome', 'variantId', 'tagVariantId', 'r']
    """

    @classmethod
    def get_schema(cls: type[LDIndex]) -> StructType:
        """Provides the schema for the LDIndex dataset.

        Returns:
            StructType: Schema for the LDIndex dataset
        """
        return parse_spark_schema("ld_index.json")
