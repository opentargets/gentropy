"""Biosample index dataset."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from functools import reduce

from gentropy.assets import data
from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset


from pyspark.sql import Column, DataFrame, Row

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

import owlready2 as owl


@dataclass
class BiosampleIndex(Dataset):
    """Biosample index dataset.

    A Biosample index dataset captures the metadata of the biosamples (e.g. tissues, cell types, cell lines, etc) such as alternate names and relationships with other biosamples.
    """

    @classmethod
    def get_schema(cls: type[StudyIndex]) -> StructType:
        """Provide the schema for the BiosampleIndex dataset.

        Returns:
            StructType: The schema of the BiosampleIndex dataset.
        """
        return parse_spark_schema("biosample_index.json")


        

