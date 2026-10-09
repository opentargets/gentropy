"""Study locus dataset."""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


class ClumpingMethod(Enum):
    """Clumping methods."""

    WC = "WINDOW_CLUMPING"
    LBC = "LOCUS_BREAKER_CLUMPING"
    LDC = "LD_CLUMPING"


class StudyLocusQualityFlag(Enum):
    """Quality control flags."""

    WINDOW_CLUMPED = "Explained by a more significant variant in the same window"
    LD_CLUMPED = ""
    NO_POPULATION = ""
    UNRESOLVED_LD = ""


class StudyLocus(Dataset):
    """Dataset representing slice of clumped summary statistics or top association.

    Depending on the creation method, this dataset contains
    * `locus` which represents the reported `SummaryStatistics`
        - build when the clumped results is annotated with SummaryStatistics.
    * `ldSet` which represents the lead locus variant (top association) imputed with LD reference panel
        - build when the clumped result is annotated with LDIndex.

    The `id` is represented as `md5('studyId', 'chromosome', 'locusStart', 'locusEnd')` and is expected to be
    unique over the study and locus boundaries.

    Examples:
        >>> locus = [("3_100501_A_T", 1.9513, -11, 0.5, 5000, 0.21, 0.01),]
        >>> ld_set = [("2_100501_G_A", 0.5),]
        >>> data = [
        ...     ("0feec5fa23383729341ed71454dc665d", "GCST0000X", "1", 100_000, 500_000, "1_100500_C_T", [], [], []),
        ...     ("991ffbcc08f240a5b694f376e10dc99d", "GCST0000X", "2", 100_000, 500_000, "2_100500_C_T", ld_set, [], []),
        ...     ("0efb6049d4b60d7e775ceb397c4927c0", "GCST0000X", "3", 100_000, 500_000, "3_100500_C_T", [], locus, []),
        ... ]
        >>> dataset = StudyLocus(_df=spark.createDataFrame(data, StudyLocus.get_schema()))
        >>> dataset.df.show(truncate=False)
        +--------------------------------+---------+----------+----------+--------+-------------+---------------------+----------------------------------------------------+---------------+
        |id                              |studyId  |chromosome|locusStart|locusEnd|leadVariantId|ldSet                |locus                                               |qualityControls|
        +--------------------------------+---------+----------+----------+--------+-------------+---------------------+----------------------------------------------------+---------------+
        |0feec5fa23383729341ed71454dc665d|GCST0000X|1         |100000    |500000  |1_100500_C_T |[]                   |[]                                                  |[]             |
        |991ffbcc08f240a5b694f376e10dc99d|GCST0000X|2         |100000    |500000  |2_100500_C_T |[{2_100501_G_A, 0.5}]|[]                                                  |[]             |
        |0efb6049d4b60d7e775ceb397c4927c0|GCST0000X|3         |100000    |500000  |3_100500_C_T |[]                   |[{3_100501_A_T, 1.9513, -11, 0.5, 5000, 0.21, 0.01}]|[]             |
        +--------------------------------+---------+----------+----------+--------+-------------+---------------------+----------------------------------------------------+---------------+
        <BLANKLINE>
    """

    ID_COLUMNS = (
        "studyId",
        "chromosome",
        "locusStart",
        "locusEnd",
    )

    @classmethod
    def get_schema(cls: type[StudyLocus]) -> StructType:
        """Provides the schema for the StudyLocus dataset.

        Returns:
            StructType: Schema for the StudyLocus dataset
        """
        return parse_spark_schema("study_locus.json")

    @classmethod
    def get_QC_column_name(cls: type[StudyLocus]) -> str:
        """Get the name of QC column."""
        return "qualityControls"

    def annotate_locus(self, SummaryStatistics) -> StudyLocus: ...

    def annotate_ld_set(self, LDIndex) -> StudyLocus: ...
