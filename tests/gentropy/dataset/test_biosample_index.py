"""Tests on Biosample index."""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
import owlready2 as owl
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, BooleanType
import json

from gentropy.dataset.biosample_index import BiosampleIndex, extract_ontology_info


def test_biosample_index_creation(mock_biosample_index: BiosampleIndex) -> None:
    """Test biosample index creation with mock biosample index."""
    assert isinstance(mock_biosample_index, BiosampleIndex)



cell_ontology = owl.get_ontology("/home/alegbe/repos/gentropy/tests/gentropy/data_samples/cell_ontology.owl").load()
spark2 = SparkSession.builder \
    .master("local[*]") \
    .appName("LocalOntologyIndexing") \
    .getOrCreate()

# Define the schema for the DataFrame
schema_path = '/home/alegbe/repos/gentropy/src/gentropy/assets/schemas/biosample_index.json'
schema = StructType.fromJson(json.load(open(schema_path)))

df = extract_ontology_info(cell_ontology, spark2, schema)