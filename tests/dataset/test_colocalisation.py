"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import dbldatagen as dg
import pytest

from otg.common.schemas import parse_spark_schema
from otg.dataset.colocalisation import Colocalisation

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture
def mock_colocalisation(spark: SparkSession) -> Colocalisation:
    """Mock colocalisation dataset."""
    schema = parse_spark_schema("colocalisation.json")

    data_spec = (
        dg.DataGenerator(
            spark,
            rows=400,
            partitions=4,
            randomSeedMethod="hash_fieldname",
            name="colocalisation",
        )
        .withSchema(schema)
        .withColumnSpec("coloc_h0", percentNulls=0.1)
        .withColumnSpec("coloc_h1", percentNulls=0.1)
        .withColumnSpec("coloc_h2", percentNulls=0.1)
        .withColumnSpec("coloc_h3", percentNulls=0.1)
        .withColumnSpec("coloc_h4", percentNulls=0.1)
        .withColumnSpec("coloc_log2_h4_h3", percentNulls=0.1)
        .withColumnSpec("clpp", percentNulls=0.1)
    )

    return Colocalisation(
        _df=data_spec.build(), path="mock_colocalisation.parquet", _schema=schema
    )


def test_colocalisation_creation(mock_colocalisation: Colocalisation) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_colocalisation, Colocalisation)
