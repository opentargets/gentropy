"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

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
        spark_config = (
            (
                SparkConf()
                .set(
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
                # .set("spark.kryoserializer.buffer", "512m")
            )
            if hail_home != "unspecified"
            else SparkConf()
        )
        self.spark = (
            SparkSession.builder.config(conf=spark_config)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.write_mode = write_mode

    def read_parquet(
        self: Session, path: str, schema: StructType, **kwargs: Dict[str, Any]
    ) -> DataFrame:
        """Reads parquet dataset with a provided schema.

        Args:
            path (str): parquet dataset path
            schema (StructType): Spark schema
            **kwargs: Additional arguments to pass to spark.read.parquet

        Returns:
            DataFrame: Dataframe with provided schema
        """
        return self.spark.read.schema(schema).parquet(path, **kwargs, inferSchema=False)  # type: ignore


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
