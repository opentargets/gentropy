"""Test study locus dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.study_locus import StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_study_locus(spark: SparkSession) -> StudyLocus:
    """Mock study_locus dataset."""
    schema = parse_spark_schema("study_locus.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="study_locus",
        )
        .withSchema(schema)
        .withColumnSpec("chromosome", percentNulls=0.1)
        .withColumnSpec("position", percentNulls=0.1)
        .withColumnSpec("beta", percentNulls=0.1)
        .withColumnSpec("oddsRatio", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("oddsRatioConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalLower", percentNulls=0.1)
        .withColumnSpec("betaConfidenceIntervalUpper", percentNulls=0.1)
        .withColumnSpec("pValueMantissa", percentNulls=0.1)
        .withColumnSpec("pValueExponent", percentNulls=0.1)
        .withColumnSpec(
            "qualityControls",
            expr="array(cast(rand() as string))",
            percentNulls=0.1,
        )
        .withColumnSpec("finemappingMethod", percentNulls=0.1)
        .withColumnSpec(
            "credibleSet",
            expr='array(named_struct("is95CredibleSet", cast(rand() > 0.5 as boolean), "is99CredibleSet", cast(rand() > 0.5 as boolean), "logABF", rand(), "posteriorProbability", rand(), "tagVariantId", cast(rand() as string), "tagPValue", rand(), "tagPValueConditioned", rand(), "tagBeta", rand(), "tagStandardError", rand(), "tagBetaConditioned", rand(), "tagStandardErrorConditioned", rand(), "r2Overall", rand()))',
            percentNulls=0.1,
        )
    )

    return StudyLocus(
        _df=data_spec.build(), path="mock_study_locus.parquet", _schema=schema
    )


def test_study_locus_creation(mock_study_locus: StudyLocus) -> None:
    """Test study locus creation with mock data."""
    assert isinstance(mock_study_locus, StudyLocus)
