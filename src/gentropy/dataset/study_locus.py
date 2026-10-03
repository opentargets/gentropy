"""Study locus dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class StudyLocus(Dataset):
    """Study-Locus dataset.

    A genomic locus in a study, defined by clumping. One row per locus per study.
    A locus holds one or more `CredibleSet` rows, and its variants live in
    `StudyLocusVariant` and `LDSet`.

    Examples:
        >>> data = [
        ...     ("sl1", "GCST000001", "1_154453788_C_T", "1", 153953788, 154953788, []),
        ...     ("sl2", "GCST000001", "2_60490000_A_G", "2", 59990000, 60990000, []),
        ...     ("sl3", "GCST000002", "6_31300000_G_T", "6", 30800000, 31800000, []),
        ...     ("sl4", "GCST000002", "19_44908684_T_C", "19", 44408684, 45408684, []),
        ... ]
        >>> study_locus = StudyLocus(_df=spark.createDataFrame(data, StudyLocus.get_schema()))
        >>> study_locus.df.show(truncate=False)
        +------------+----------+-----------------+----------+----------+---------+---------------+
        |studyLocusId|studyId   |sentinelVariantId|chromosome|locusStart|locusEnd |qualityControls|
        +------------+----------+-----------------+----------+----------+---------+---------------+
        |sl1         |GCST000001|1_154453788_C_T  |1         |153953788 |154953788|[]             |
        |sl2         |GCST000001|2_60490000_A_G   |2         |59990000  |60990000 |[]             |
        |sl3         |GCST000002|6_31300000_G_T   |6         |30800000  |31800000 |[]             |
        |sl4         |GCST000002|19_44908684_T_C  |19        |44408684  |45408684 |[]             |
        +------------+----------+-----------------+----------+----------+---------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset.

        Returns:
            StructType: Schema for the StudyLocus dataset
        """
        return parse_spark_schema("study_locus.json")
