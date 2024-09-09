"""Biosample index for Cell Ontology data source."""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import owlready2 as owl

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex, extract_ontology_info

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column

class CellOntologyBiosampleIndex:
    """Biosample index dataset from Cell Ontology.
    
    Cell type data is extracted from the Cell Ontology (CL) https://obophenotype.github.io/cell-ontology/ and used to define the cell types in the biosample index dataset.
    """

    @classmethod
    def extract_celltypes_from_source(
        cls: type[CellOntologyStudyIndex],
        session: Session,
        ontology_path: str,
    ) -> DataFrame:
        """Ingests Cell Ontology owo file and extracts cell types.

        Args:
            session (Session): Spark session.
            ontology_path (str): Path to the Cell ontology owo file.

        Returns:
            BiosampleIndex: Parsed and annotated Cell Ontology biosample index table.
        """
        ontology_data = owl.get_ontology(ontology_path).load()
        df = extract_ontology_info(ontology_data, "CL_", session, BiosampleIndex.get_schema())
        
        return BiosampleIndex(
            _df=df,
            _schema=BiosampleIndex.get_schema()
            )