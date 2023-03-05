"""Test study locus overlap dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_study_locus_overlap(spark: SparkSession) -> StudyLocusOverlap:
    """Mock study_locus_overlap dataset."""
    schema = parse_spark_schema("study_locus_overlap.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="study_locus",
        )
        .withSchema(schema)
        .withColumnSpec("right_logABF", percentNulls=0.1)
        .withColumnSpec("left_logABF", percentNulls=0.1)
        .withColumnSpec("right_posteriorProbability", percentNulls=0.1)
        .withColumnSpec("left_posteriorProbability", percentNulls=0.1)
    )

    return StudyLocusOverlap(
        _df=data_spec.build(), path="mock_study_locus_overlap.parquet", _schema=schema
    )


def test_study_locus_overlap_creation(mock_study_locus: StudyLocusOverlap) -> None:
    """Test study locus overlap creation with mock data."""
    assert isinstance(mock_study_locus, StudyLocusOverlap)
