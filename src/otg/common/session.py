"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class ETLSession:
    """Spark session class."""

    spark_config = SparkConf()

    def __init__(
        self: ETLSession,
        spark_uri: str,
        app_name: str,
        write_mode: str,
        spark_config: SparkConf = spark_config,
    ) -> None:
        """Initialises spark session and logger.

        Args:
            spark_uri (str): spark uri
            app_name (str): spark application name
            write_mode (str): spark write mode
            spark_config (SparkConf): spark configuration. Defaults to spark_config.
        """
        # create session and retrieve Spark logger object
        self.spark = (
            SparkSession.builder.config(conf=spark_config)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.write_mode = write_mode

    def read_parquet(self: ETLSession, path: str, schema: StructType) -> DataFrame:
        """Reads parquet dataset with a provided schema.

        Args:
            path (str): parquet dataset path
            schema (StructType): Spark schema

        Returns:
            DataFrame: Dataframe with provided schema
        """
        return self.spark.read.schema(schema).format("parquet").load(path)


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
