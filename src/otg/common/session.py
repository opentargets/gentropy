"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from psutil import virtual_memory
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class Session:
    """Spark session class."""

    def __init__(
        self: Session,
        spark_uri: str = "local[*]",
        write_mode: str = "errorifexists",
        app_name: str = "otgenetics",
        hail_home: str = "unspecified",
    ) -> None:
        """Initialises spark session and logger.

        Args:
            spark_uri (str): spark uri
            app_name (str): spark application name
            write_mode (str): spark write mode
            hail_home (str): path to hail installation
        """
        # create session and retrieve Spark logger object
        total_memory = self.detect_spark_memory_limit()
        executor_memory = 50 if total_memory >= 500 else 20
        driver_memory_limit = int(0.15 * total_memory)

        # create executors based on resources
        total_cores = os.cpu_count() or 16
        reserved_cores = 5  # for OS and other processes
        cores_per_executor = 8
        max_executors = int((total_cores - reserved_cores) / cores_per_executor)
        default_spark_conf = (
            SparkConf()
            .set("spark.driver.memory", f"{driver_memory_limit}g")
            .set("spark.executor.memory", f"{executor_memory}g")
            # Dynamic allocation
            .set("spark.dynamicAllocation.enabled", "true")
            .set("spark.dynamicAllocation.minExecutors", "2")
            .set("spark.dynamicAllocation.maxExecutors", str(max_executors))
            .set("spark.dynamicAllocation.initialExecutors", "2")
            .set(
                "spark.shuffle.service.enabled", "true"
            )  # required for dynamic allocation
        )
        spark_config = (
            (
                default_spark_conf.set(
                    "spark.jars",
                    f"{hail_home}/backend/hail-all-spark.jar",
                )
                .set(
                    "spark.driver.extraClassPath",
                    f"{hail_home}/backend/hail-all-spark.jar",
                )
                .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
                .set("spark.sql.files.openCostInBytes", "50gb")
                .set("spark.sql.files.maxPartitionBytes", "50gb")
                # .set("spark.kryoserializer.buffer", "512m")
            )
            if hail_home != "unspecified"
            else default_spark_conf
        )
        self.spark = (
            SparkSession.builder.config(conf=spark_config)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.write_mode = write_mode

    @staticmethod
    def detect_spark_memory_limit() -> int:
        """Detect the total amount of physical memory and allow Spark to use (almost) all of it."""
        mem_gib = virtual_memory().total >> 30
        return int(mem_gib * 0.7)

    def read_parquet(self: Session, path: str, schema: StructType) -> DataFrame:
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
        log4j = spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        self.logger = log4j.Logger.getLogger(__name__)

        log4j_logger = spark.sparkContext._jvm.org.apache.log4j  # type: ignore
        self.logger = log4j_logger.LogManager.getLogger(__name__)

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
