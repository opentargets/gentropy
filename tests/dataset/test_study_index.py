"""Test study index dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_study_index(spark: SparkSession) -> StudyIndex:
    """Mock v2g dataset."""
    schema = parse_spark_schema("studies.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="v2g",
        )
        .withSchema(schema)
        .withColumnSpec(
            "traitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec(
            "backgroundTraitFromSourceMappedIds",
            expr="array(cast(rand() AS string))",
            percentNulls=0.1,
        )
        .withColumnSpec("geneId", percentNulls=0.1)
        .withColumnSpec(
            "discoverySamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec(
            "replicationSamples",
            expr='array(named_struct("sampleSize", cast(rand() as string), "ancestry", cast(rand() as string)))',
            percentNulls=0.1,
        )
        .withColumnSpec("pubmedId", percentNulls=0.1)
        .withColumnSpec("publicationFirstAuthor", percentNulls=0.1)
        .withColumnSpec("publicationDate", percentNulls=0.1)
        .withColumnSpec("publicationJournal", percentNulls=0.1)
        .withColumnSpec("publicationTitle", percentNulls=0.1)
        .withColumnSpec("initialSampleSize", percentNulls=0.1)
        .withColumnSpec("nCases", percentNulls=0.1)
        .withColumnSpec("nControls", percentNulls=0.1)
        .withColumnSpec("nSamples", percentNulls=0.1)
        .withColumnSpec("summarystatsLocation", percentNulls=0.1)
    )

    return StudyIndex(
        _df=data_spec.build(), path="mock_study_index.parquet", _schema=schema
    )


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)
