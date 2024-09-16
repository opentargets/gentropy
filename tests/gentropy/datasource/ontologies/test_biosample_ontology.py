"""Tests for study index dataset from FinnGen."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


from gentropy.dataset.study_index import BiosampleIndex
from gentropy.datasource.ontologies.utils import extract_ontology_from_json, merge_biosample_indices

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

def test_biosample_index_from_source(spark: SparkSession) -> None:
    """Test biosample index from source."""
    assert isinstance(extract_ontology_from_json(), BiosampleIndex)

class TestOntologyParger:
    """ Testing ontology parser."""

    SAMPLE_CELL_ONTOLOGY_PATH = "tests/gentropy/data_samples/cell_ontology_sample.json"
    SAMPLE_UBERON_PATH = "tests/gentropy/data_samples/uberon_sample.json"

    def test_cell_ontology_parser(self) -> None:
        """Test cell ontology parser."""
        cell_ontology = extract_ontology_from_json(self.SAMPLE_CELL_ONTOLOGY_PATH)
        assert isinstance(
            cell_ontology, BiosampleIndex
            ), "Cell ontology subset is not parsed correctly to BiosampleIndex."

    def test_uberon_parser(self) -> None:
        """Test uberon parser."""
        uberon = extract_ontology_from_json(self.SAMPLE_UBERON_PATH)
        assert isinstance(
            uberon, BiosampleIndex
            ), "Uberon subset is not parsed correctly to BiosampleIndex."

    def test_merge_biosample_indices(self) -> None:
        """Test merging of biosample indices."""
        cell_ontology = extract_ontology_from_json(self.SAMPLE_CELL_ONTOLOGY_PATH)
        uberon = extract_ontology_from_json(self.SAMPLE_UBERON_PATH)
        merged = merge_biosample_indices(cell_ontology, uberon)
        assert isinstance(
            merged, BiosampleIndex
            ), "Merging of biosample indices is not correct."