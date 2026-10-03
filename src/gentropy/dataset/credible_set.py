"""Credible set dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class CredibleSet(Dataset):
    """Credible set dataset.

    One row per fine-mapping result. Each credible set belongs to one `StudyLocus`,
    and its variants live in `CredibleSetVariant`. `leadVariantId` is the variant
    with the highest posterior probability in the credible set.

    !!! note

        `confidence` is a byte-encoded enumeration. Its mapping is defined in a
        later change.

    Examples:
        >>> data = [
        ...     ("cs1", "sl1", "1_154453788_C_T", "SuSiE", 1, 12.5, 1, 0.92, 0.85, False, []),
        ...     ("cs2", "sl1", "1_154600000_A_C", "SuSiE", 2, 8.3, 1, 0.88, 0.79, False, []),
        ...     ("cs3", "sl1", "1_154453788_C_T", "PICS", None, None, 3, None, None, False, []),
        ...     ("cs4", "sl2", "2_60490000_A_G", "PICS", None, None, 3, None, None, False, []),
        ... ]
        >>> credible_set = CredibleSet(_df=spark.createDataFrame(data, CredibleSet.get_schema()))
        >>> credible_set.df.show(truncate=False)
        +-------------+------------+---------------+-----------------+-----------------+----------------+----------+------------+-----------+----------+---------------+
        |credibleSetId|studyLocusId|leadVariantId  |fineMappingMethod|singleEffectIndex|log10BayesFactor|confidence|purityMeanR2|purityMinR2|isTransQtl|qualityControls|
        +-------------+------------+---------------+-----------------+-----------------+----------------+----------+------------+-----------+----------+---------------+
        |cs1          |sl1         |1_154453788_C_T|SuSiE            |1                |12.5            |1         |0.92        |0.85       |false     |[]             |
        |cs2          |sl1         |1_154600000_A_C|SuSiE            |2                |8.3             |1         |0.88        |0.79       |false     |[]             |
        |cs3          |sl1         |1_154453788_C_T|PICS             |NULL             |NULL            |3         |NULL        |NULL       |false     |[]             |
        |cs4          |sl2         |2_60490000_A_G |PICS             |NULL             |NULL            |3         |NULL        |NULL       |false     |[]             |
        +-------------+------------+---------------+-----------------+-----------------+----------------+----------+------------+-----------+----------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[CredibleSet]) -> StructType:
        """Provides the schema for the CredibleSet dataset.

        Returns:
            StructType: Schema for the CredibleSet dataset
        """
        return parse_spark_schema("credible_set.json")
