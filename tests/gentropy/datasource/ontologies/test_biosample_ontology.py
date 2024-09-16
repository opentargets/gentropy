"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy.dataset.study_index import BiosampleIndex
from gentropy.datasource.ontologies.utils import extract_ontology_from_json


def test_biosample_index_from_source(spark: SparkSession) -> None:
    """Test biosample index from source."""
    assert isinstance(extract_ontology_from_json(), BiosampleIndex)
