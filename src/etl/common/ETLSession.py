from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from etl.common import Log4j
from etl.json import schemas

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from pyspark.sql import DataFrame


class ETLSession:
    def __init__(self: ETLSession, cfg: DictConfig) -> None:
        # create session and retrieve Spark logger object
        self.spark = (
            SparkSession.builder.master(cfg.environment.sparkUri)
            .appName(cfg.etl.name)
            .getOrCreate()
        )

        self.logger = Log4j.Log4j(self.spark)

    def read_parquet(self: ETLSession, path: str, schema_json: str) -> DataFrame:
        """
        Reads a parquet file from the given path and returns a DataFrame with the given schema

        Args:
          self (ETLSession): ETLSession
          path (str): The path to the parquet file.
          schema_json (str): The name of the schema file in the schemas package.

        Returns:
          A DataFrame
        """
        core_schema = json.loads(
            pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
        )
        schema = StructType.fromJson(core_schema)
        return self.spark.read.schema(schema).format("parquet").load(path)
