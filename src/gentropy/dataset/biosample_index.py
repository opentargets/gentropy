"""Study index dataset."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from gentropy.assets import data
from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


@dataclass
class StudyIndex(Dataset):
    """Study index dataset.

    A study index dataset captures all the metadata for all studies including GWAS and Molecular QTL.
    """

    @classmethod
    def get_schema(cls: type[StudyIndex]) -> StructType:
        """Provide the schema for the BioSampleIndex dataset.

        Returns:
            StructType: The schema of the BioSampleIndex dataset.
        """
        return parse_spark_schema("biosample_index.json")
