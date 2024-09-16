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



spark = SparkSession.builder \
    .master("local[*]") \
    .appName("LocalOntologyIndexing") \
    .getOrCreate()

ontology_json1 = "file:////home/alegbe/repos/gentropy/tests/gentropy/data_samples/nephron-minimal.json"
ontology_json2 = "file://///home/alegbe/repos/gentropy/tests/gentropy/data_samples/cell_ontology_dummy.json"

df1 = extract_ontology_from_json(ontology_json1, spark)
df2 = extract_ontology_from_json(ontology_json2, spark)

df_merged = merge_biosample_indices([df1, df2])
