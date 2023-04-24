"""Test PICS finemapping."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from otg.dataset.study_locus import StudyLocus
from otg.method.pics import PICS


def test_pics(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)


@pytest.mark.parametrize(
    ("pics_postprob", "expected"),
    [(1.0, True), (0.95, True), (0.9, False), (0.5, False), (0.0, False)],
)
def test_is_in_credset(
    spark: SparkSession, pics_postprob: float, expected: bool
) -> None:
    """Test _is_in_credset over a range of probabilities."""
    df = spark.createDataFrame(
        [("1", "A", "varA", pics_postprob)],
        ["chromosome", "study_id", "variant_id", "pics_postprob"],
    )

    result = df.withColumn(
        "is_in_credset",
        PICS._is_in_credset(
            f.col("chromosome"),
            f.col("study_id"),
            f.col("variant_id"),
            f.col("pics_postprob"),
            0.95,
        ),
    ).first()["is_in_credset"]
    assert result == expected
