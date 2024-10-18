"""Test dataset validation/exclusion."""

from __future__ import annotations

import pyspark.sql.functions as f
import pytest
from pyspark.sql import SparkSession

from gentropy.dataset.study_index import StudyIndex, StudyQualityCheck


class TestDataExclusion:
    """Testing Dataset exclusion.

    Calling `dataset.valid_rows` methods on a mock datasets to test if
    the right rows are excluded.
    """

    CORRECT_FLAG = ["DUPLICATED_STUDY"]
    INCORRECT_FLAG = ["UNKNOWN_CATEGORY"]
    ALL_FLAGS = [member.name for member in StudyQualityCheck]

    DATASET = [
        # Good study no flag:
        ("S1", None),
        # Good study permissive flag:
        ("S2", "This type of study is not supported"),
        ("S2", "No valid disease identifier found"),
        # Bad study:
        ("S3", "The identifier of this study is not unique"),
        ("S3", "This type of study is not supported"),
    ]

    @pytest.fixture(autouse=True)
    def _setup(self: TestDataExclusion, spark: SparkSession) -> None:
        """Setup study the mock index for testing."""
        self.study_index = StudyIndex(
            _df=(
                spark.createDataFrame(self.DATASET, ["studyId", "flag"])
                .groupBy("studyId")
                .agg(f.collect_list("flag").alias("qualityControls"))
                .select(
                    "studyId",
                    "qualityControls",
                    f.lit("project1").alias("projectId"),
                    f.lit("gwas").alias("studyType"),
                )
            ),
            _schema=StudyIndex.get_schema(),
        )

    @pytest.mark.parametrize(
        "filter_, expected",
        [
            (CORRECT_FLAG, ["S1", "S2"]),
            (ALL_FLAGS, ["S1"]),
        ],
    )
    def test_valid_rows(
        self: TestDataExclusion, filter_: list[str], expected: list[str]
    ) -> None:
        """Test valid rows."""
        passing_studies = [
            study["studyId"]
            for study in self.study_index.valid_rows(
                filter_, invalid=False
            ).df.collect()
        ]

        assert passing_studies == expected

    @pytest.mark.parametrize(
        "filter_, expected",
        [
            (CORRECT_FLAG, ["S3"]),
            (ALL_FLAGS, ["S2", "S3"]),
        ],
    )
    def test_invalid_rows(
        self: TestDataExclusion, filter_: list[str], expected: list[str]
    ) -> None:
        """Test invalid rows."""
        failing_studies = [
            study["studyId"]
            for study in self.study_index.valid_rows(filter_, invalid=True).df.collect()
        ]

        assert failing_studies == expected

    def test_failing_quality_flag(self: TestDataExclusion) -> None:
        """Test invalid quality flag."""
        with pytest.raises(ValueError):
            self.study_index.valid_rows(self.INCORRECT_FLAG, invalid=True).df.collect()

        with pytest.raises(ValueError):
            self.study_index.valid_rows(self.INCORRECT_FLAG, invalid=False).df.collect()
