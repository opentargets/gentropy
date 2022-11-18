"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from etl.json import schemas

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from pyspark.sql import DataFrame


class ETLSession:
    """Spark session class."""

    spark_config = SparkConf()

    def __init__(
        self: ETLSession, cfg: DictConfig, spark_config: SparkConf = spark_config
    ) -> None:
        """Initialises spark session and logger.

        Args:
            cfg (DictConfig): configuration file
            spark_config (SparkConf): Optional spark config. Defaults to spark_config.
        """
        # create session and retrieve Spark logger object
        self.spark = (
            SparkSession.builder.config(conf=spark_config)
            .master(cfg.environment.sparkUri)
            .appName(cfg.etl.name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)

    def read_parquet(self: ETLSession, path: str, schema_json: str) -> DataFrame:
        """Reads parquet dataset with a provided schema.

        Args:
            path (str): parquet dataset path
            schema_json (str): name of the schema file (e.g. "schema.json")

        Returns:
            DataFrame: Dataframe with provided schema
        """
        core_schema = json.loads(
            pkg_resources.read_text(schemas, schema_json, encoding="utf-8")
        )
        schema = StructType.fromJson(core_schema)
        df = self.spark.read.schema(schema).format("parquet").load(path)

        return df


class Log4j:
    """Log4j logger class."""

    def __init__(self: Log4j, spark: SparkSession) -> None:
        """Initialise logger.

        Args:
            spark (SparkSession): Available spark session
        """
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get("spark.app.id")
        app_name = conf.get("spark.app.name")

        log4j = spark._jvm.org.apache.log4j

        message_prefix = f"<{app_name}-{app_id}>"
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self: Log4j, message: str) -> None:
        """Log an error.

        Args:
            message (str): Error message to write to log

        Returns:
            _type_: None
        """
        self.logger.error(message)
        return None

    def warn(self: Log4j, message: str) -> None:
        """Log a warning.

        Args:
            message (str): Warning messsage to write to log

        Returns:
            _type_: None
        """
        self.logger.warn(message)
        return None

    def info(self: Log4j, message: str) -> None:
        """Log information.

        Args:
            message (str): Information message to write to log

        Returns:
            _type_: None
        """
        self.logger.info(message)
        return None
