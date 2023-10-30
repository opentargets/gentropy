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
        hail_home: str | None = None,
        extended_spark_conf: dict | None = None,
    ) -> None:
        """Initialises spark session and logger.

        Args:
            spark_uri (str): spark uri
            app_name (str): spark application name
            write_mode (str): spark write mode
            hail_home (str | None): path to hail installation
            extended_spark_conf (dict): extended spark configuration
        """
        merged_conf = self._create_merged_config(hail_home, extended_spark_conf)

        self.spark = (
            SparkSession.builder.config(conf=merged_conf)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.write_mode = write_mode

    def _default_config(self: Session) -> SparkConf:
        """Default spark configuration."""
        return (
            SparkConf()
            # Dynamic allocation
            .set("spark.dynamicAllocation.enabled", "true")
            .set("spark.dynamicAllocation.minExecutors", "2")
            .set("spark.dynamicAllocation.initialExecutors", "2")
            .set(
                "spark.shuffle.service.enabled", "true"
            )  # required for dynamic allocation
        )

    def _hail_config(self: Session, hail_home: str | None) -> SparkConf:
        """Returns the Hail specific Spark configuration."""
        if hail_home is None:
            return SparkConf()
        return (
            SparkConf()
            .set("spark.jars", f"{hail_home}/backend/hail-all-spark.jar")
            .set(
                "spark.driver.extraClassPath", f"{hail_home}/backend/hail-all-spark.jar"
            )
            .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
            .set("spark.sql.files.openCostInBytes", "50gb")
            .set("spark.sql.files.maxPartitionBytes", "50gb")
        )

    def _create_merged_config(
        self: Session, hail_home: str | None, extended_spark_conf: SparkConf
    ) -> SparkConf:
        """Merges the default, and optionally the Hail and extended configurations if provided."""
        all_settings = (
            self._default_config().getAll()
            + self._hail_config(hail_home).getAll()
            + list(extended_spark_conf.items())
            if extended_spark_conf
            else []
        )
        return SparkConf().setAll(all_settings)

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
        return self.spark.read.schema(schema).parquet(path, **kwargs)


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
