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
from gentropy.datasource.ontologies.utils import extract_ontology_from_json


def test_biosample_index_creation(mock_biosample_index: BiosampleIndex) -> None:
    """Test biosample index creation with mock biosample index."""
    assert isinstance(mock_biosample_index, BiosampleIndex)



spark2 = SparkSession.builder \
    .master("local[*]") \
    .appName("LocalOntologyIndexing") \
    .getOrCreate()


ontology_json = 'file:///home/alegbe/cl.json'
# ontology_json = 'file:///home/alegbe/uberon.json'

df = extract_ontology_from_json(ontology_json, spark2)