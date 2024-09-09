"""Biosample index for Cell Ontology data source."""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import owlready2 as owl

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column

class CellOntologyStudyIndex:
    """Study index dataset from Cell Ontology.
    
    Cell type data is extracted from the Cell Ontology (CL) https://obophenotype.github.io/cell-ontology/ and used to define the cell types in the study index dataset.

    """"

    # Define the schema explicitly for the DataFrame
    raw_biosample_schema: StructType = StructType(
        [
            StructField("id", StringType(), True),
            StructField("code", StringType(), True),
            StructField("name", StringType(), True),
            StructField("dbXRefs", ArrayType(StringType()), True),
            StructField("description", StringType(), True),
            StructField("parents", ArrayType(StringType()), True),
            StructField("synonyms", ArrayType(StringType()), True),
            StructField("ancestors", ArrayType(StringType()), True),
            StructField("descendants", ArrayType(StringType()), True),
            StructField("children", ArrayType(StringType()), True),
            StructField("ontology", MapType(StringType(), BooleanType()), True)
        ]
    )
    raw_biosample_path = "https://raw.githubusercontent.com/obophenotype/cell-ontology/master/cl.owl" # Dummy path for now

    @classmethod
    def extract_celltypes_from_source(
        cls: type[CellOntologyStudyIndex],
        session: Session,
        mqtl_quantification_methods_blacklist: list[str],
    ) -> DataFrame:
        """Read raw studies metadata from eQTL Catalogue.

        Args:
            session (Session): Spark session.
            mqtl_quantification_methods_blacklist (list[str]): Molecular trait quantification methods that we don't want to ingest. Available options in https://github.com/eQTL-Catalogue/eQTL-Catalogue-resources/blob/master/data_tables/dataset_metadata.tsv

        Returns:
            DataFrame: raw studies metadata.
        """
        pd.DataFrame.iteritems = pd.DataFrame.items
        return session.spark.createDataFrame(
            pd.read_csv(cls.raw_studies_metadata_path, sep="\t"),
            schema=cls.raw_studies_metadata_schema,
        ).filter(~(f.col("quant_method").isin(mqtl_quantification_methods_blacklist)))
