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
        >>> data = [("sl1", "1_154453788_C_T", 1.0, -10, 0.25, 10000, (1, 0.12, 0.02), None, [])]
        >>> study_locus_variant = StudyLocusVariant(_df=spark.createDataFrame(data, StudyLocusVariant.get_schema()))
        >>> study_locus_variant.df.show(truncate=False)
        +------------+---------------+--------------+--------------+---------------------+----------+---------------+--------------+---------------+
        |studyLocusId|variantId      |pValueMantissa|pValueExponent|effectAlleleFrequency|sampleSize|reportedEffect |rescaledEffect|qualityControls|
        +------------+---------------+--------------+--------------+---------------------+----------+---------------+--------------+---------------+
        |sl1         |1_154453788_C_T|1.0           |-10           |0.25                 |10000     |{1, 0.12, 0.02}|NULL          |[]             |
        +------------+---------------+--------------+--------------+---------------------+----------+---------------+--------------+---------------+
        <BLANKLINE>
    """

    @classmethod
    def get_schema(cls: type[StudyLocusVariant]) -> StructType:
        """Provides the schema for the StudyLocusVariant dataset.

        Returns:
            StructType: Schema for the StudyLocusVariant dataset
        """
        return parse_spark_schema("study_locus_variant.json")
