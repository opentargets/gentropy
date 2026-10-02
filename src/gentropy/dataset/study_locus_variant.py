"""Study locus variant dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class StudyLocusVariant(Dataset):
    """Study-Locus variant dataset.

    One row per variant per `StudyLocus`, holding the summary statistics the study
    reports for the variants in the locus region. `reportedEffect` carries the effect
    as reported by the study, and `rescaledEffect` the effect derived from it.

    Examples:
        >>> data = [
        ...     ("sl1", "1_154453788_C_T", 1.0, -10, 0.25, 10000, (1, 0.12, 0.02), None, []),
        ...     ("sl1", "1_154460000_G_A", 3.2, -8, 0.31, 10000, (1, 0.09, 0.016), None, []),
        ...     ("sl2", "2_60490000_A_G", 2.5, -12, 0.42, 10000, (-1, 0.15, 0.021), None, []),
        ...     ("sl2", "2_60500000_T_C", 7.1, -9, 0.38, 10000, (-1, 0.11, 0.019), None, []),
        ... ]
        >>> study_locus_variant = StudyLocusVariant(_df=spark.createDataFrame(data, StudyLocusVariant.get_schema()))
        >>> study_locus_variant.df.show(truncate=False)
        +------------+---------------+--------------+--------------+---------------------+----------+-----------------+--------------+---------------+
        |studyLocusId|variantId      |pValueMantissa|pValueExponent|effectAlleleFrequency|sampleSize|reportedEffect   |rescaledEffect|qualityControls|
        +------------+---------------+--------------+--------------+---------------------+----------+-----------------+--------------+---------------+
        |sl1         |1_154453788_C_T|1.0           |-10           |0.25                 |10000     |{1, 0.12, 0.02}  |NULL          |[]             |
        |sl1         |1_154460000_G_A|3.2           |-8            |0.31                 |10000     |{1, 0.09, 0.016} |NULL          |[]             |
        |sl2         |2_60490000_A_G |2.5           |-12           |0.42                 |10000     |{-1, 0.15, 0.021}|NULL          |[]             |
        |sl2         |2_60500000_T_C |7.1           |-9            |0.38                 |10000     |{-1, 0.11, 0.019}|NULL          |[]             |
        +------------+---------------+--------------+--------------+---------------------+----------+-----------------+--------------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[StudyLocusVariant]) -> StructType:
        """Provides the schema for the StudyLocusVariant dataset.

        Returns:
            StructType: Schema for the StudyLocusVariant dataset
        """
        return parse_spark_schema("study_locus_variant.json")
