"""Test the fine-mapping study metadata dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from gentropy.dataset.fine_mapping_study_metadata import FineMappingStudyMetadata

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestFineMappingStudyMetadata:
    """Test one-row-per-study metadata behavior."""

    def test_accepts_one_row_per_study(self, spark: SparkSession) -> None:
        """Three studies are represented by three metadata rows."""
        metadata = FineMappingStudyMetadata(
            _df=spark.createDataFrame(
                [
                    ("study-1", "EUR", 100),
                    ("study-2", "AFR", 200),
                    ("study-3", "CSA", 300),
                ],
                ["studyId", "ancestry", "sampleSize"],
            ),
            _schema=FineMappingStudyMetadata.get_schema(),
        )

        assert metadata.df.count() == 3

    def test_rejects_duplicate_study(self, spark: SparkSession) -> None:
        """A study cannot have multiple metadata records."""
        with pytest.raises(AssertionError, match="one row per studyId"):
            FineMappingStudyMetadata(
                _df=spark.createDataFrame(
                    [("study-1", "EUR", 100), ("study-1", "AFR", 200)],
                    ["studyId", "ancestry", "sampleSize"],
                ),
                _schema=FineMappingStudyMetadata.get_schema(),
            )
