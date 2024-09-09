"""Biosample index for Uberon data source."""

from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import owlready2 as owl

from gentropy.common.session import Session
from gentropy.dataset.biosample_index import BiosampleIndex

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.column import Column

class UberonBiosampleIndex:
    """Biosample index dataset from Uberon.
    
    Cell type data is extracted from the Uberon (UBERON) https://obophenotype.github.io/uberon/ and used to define the tissues in the biosample index dataset.
    """

    @classmethod
    def extract_tissue_from_source(
        cls: type[UberonStudyIndex],
        session: Session,
        ontology_path: str,
    ) -> DataFrame:
        """Ingests Uberon owo file and extracts tissues.

        Args:
            session (Session): Spark session.
            ontology_path (str): Path to the Uberon owo file.

        Returns:
            BiosampleIndex: Parsed and annotated Uberon biosample index table.
        """
        ontology_data = owl.get_ontology(ontology_path).load()
        df = extract_ontology_info(ontology_data, "UBERON_", session, BiosampleIndex.get_schema())
        
        return BiosampleIndex(
            _df=df,
            _schema=BiosampleIndex.get_schema()
            )
