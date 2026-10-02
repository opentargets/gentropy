"""LD set dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class LDSet(Dataset):
    """Linkage disequilibrium set dataset.

    One row per tagging variant per `StudyLocus`. `r2Overall` is the correlation
    between the tag variant and the locus sentinel, and `ldPopulation` the ancestry
    reference that produced it.

    Examples:
        >>> data = [
        ...     ("sl1", "1_154460000_G_A", 0.85, "nfe", []),
        ...     ("sl1", "1_154470000_T_G", 0.62, "nfe", []),
        ...     ("sl2", "2_60500000_T_C", 0.91, "afr", []),
        ...     ("sl2", "2_60510000_C_A", 0.74, "afr", []),
        ... ]
        >>> ld_set = LDSet(_df=spark.createDataFrame(data, LDSet.get_schema()))
        >>> ld_set.df.show(truncate=False)
        +------------+---------------+---------+------------+---------------+
        |studyLocusId|tagVariantId   |r2Overall|ldPopulation|qualityControls|
        +------------+---------------+---------+------------+---------------+
        |sl1         |1_154460000_G_A|0.85     |nfe         |[]             |
        |sl1         |1_154470000_T_G|0.62     |nfe         |[]             |
        |sl2         |2_60500000_T_C |0.91     |afr         |[]             |
        |sl2         |2_60510000_C_A |0.74     |afr         |[]             |
        +------------+---------------+---------+------------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[LDSet]) -> StructType:
        """Provides the schema for the LDSet dataset.

        Returns:
            StructType: Schema for the LDSet dataset
        """
        return parse_spark_schema("ld_set.json")
