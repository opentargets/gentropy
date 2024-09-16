"""Tests on Biosample index."""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
import owlready2 as owl
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, BooleanType
import json

from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.datasource.ontologies.utils import extract_ontology_from_json, merge_biosample_indices


def test_biosample_index_creation(mock_biosample_index: BiosampleIndex) -> None:
    """Test biosample index creation with mock biosample index."""
    assert isinstance(mock_biosample_index, BiosampleIndex)

