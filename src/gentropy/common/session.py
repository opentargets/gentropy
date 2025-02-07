"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class Session:
    """This class provides a Spark session and logger."""

    def __init__(  # noqa: D107
        self: Session,
        spark_uri: str = "local[*]",
        write_mode: str = "errorifexists",
        app_name: str = "gentropy",
        hail_home: str | None = None,
        start_hail: bool = False,
        extended_spark_conf: dict[str, str] | None = None,
        output_partitions: int = 200,
    ) -> None:
        """Initialises spark session and logger.

        Args:
            spark_uri (str): Spark URI. Defaults to "local[*]".
            write_mode (str): Spark write mode. Defaults to "errorifexists".
            app_name (str): Spark application name. Defaults to "gentropy".
            hail_home (str | None): Path to Hail installation. Defaults to None.
            start_hail (bool): Whether to start Hail. Defaults to False.
            extended_spark_conf (dict[str, str] | None): Extended Spark configuration. Defaults to None.
            output_partitions (int): Number of partitions for output datasets. Defaults to 200.
        """
        merged_conf = self._create_merged_config(
            start_hail, hail_home, extended_spark_conf
        )

        self.spark = (
            SparkSession.Builder()
            .config(conf=merged_conf)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(self.spark)

        self.write_mode = write_mode

        self.hail_home = hail_home
        self.start_hail = start_hail
        if start_hail:
            hl.init(sc=self.spark.sparkContext, log="/dev/null")
        self.output_partitions = output_partitions

    def _default_config(self: Session) -> SparkConf:
        """Default spark configuration.

        Returns:
            SparkConf: Default spark configuration.
        """
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

    def _hail_config(
        self: Session, start_hail: bool, hail_home: str | None
    ) -> SparkConf:
        """Returns the Hail specific Spark configuration.

        Args:
            start_hail (bool): Whether to start Hail.
            hail_home (str | None): Path to Hail installation.

        Returns:
            SparkConf: Hail specific Spark configuration.

        Raises:
            ValueError: If Hail home is not specified but Hail is requested.
        """
        if not start_hail:
            return SparkConf()
        if not hail_home:
            raise ValueError("Hail home must be specified to start Hail.")
        return (
            SparkConf()
            .set("spark.jars", f"{hail_home}/backend/hail-all-spark.jar")
            .set(
                "spark.driver.extraClassPath", f"{hail_home}/backend/hail-all-spark.jar"
            )
            .set("spark.executor.extraClassPath", "./hail-all-spark.jar")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
        )

    def _create_merged_config(
        self: Session,
        start_hail: bool,
        hail_home: str | None,
        extended_spark_conf: dict[str, str] | None,
    ) -> SparkConf:
        """Merges the default, and optionally the Hail and extended configurations if provided.

        Args:
            start_hail (bool): Whether to start Hail.
            hail_home (str | None): Path to Hail installation. Defaults to None.
            extended_spark_conf (dict[str, str] | None): Extended Spark configuration.

        Returns:
            SparkConf: Merged Spark configuration.
        """
        all_settings = (
            self._default_config().getAll()
            + self._hail_config(start_hail, hail_home).getAll()
        )
        if extended_spark_conf:
            all_settings += list(extended_spark_conf.items())
        return SparkConf().setAll(all_settings)

    def load_data(
        self: Session,
        path: str | list[str],
        format: str = "parquet",
        schema: StructType | str | None = None,
        **kwargs: bool | float | int | str | None,
    ) -> DataFrame:
        """Generic function to read a file or folder into a Spark dataframe.

        The `recursiveFileLookup` flag when set to True will skip all partition columns, but read files from all subdirectories.

        Args:
            path (str | list[str]): path to the dataset
            format (str): file format. Defaults to parquet.
            schema (StructType | str | None): Schema to use when reading the data.
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.load. `mergeSchema` is set to True, `recursiveFileLookup` is set to False by default.

        Returns:
            DataFrame: Dataframe
        """
        # Set default kwargs
        if schema is None:
            kwargs["inferSchema"] = kwargs.get("inferSchema", True)
        kwargs["mergeSchema"] = kwargs.get("mergeSchema", True)
        kwargs["recursiveFileLookup"] = kwargs.get("recursiveFileLookup", False)
        return self.spark.read.load(path, format=format, schema=schema, **kwargs)


class Log4j:
    """Log4j logger class."""

    def __init__(self, spark: SparkSession) -> None:
        """Log4j logger class. This class provides a wrapper around the Log4j logging system.

        Args:
            spark (SparkSession): The Spark session used to access Spark context and Log4j logging.
        """
        # get spark app details with which to prefix all messages
        log4j = spark.sparkContext._jvm.org.apache.log4j  # type: ignore[assignment, unused-ignore]
        self.logger = log4j.Logger.getLogger(__name__)

        log4j_logger = spark.sparkContext._jvm.org.apache.log4j  # type: ignore[assignment, unused-ignore]
        self.logger = log4j_logger.LogManager.getLogger(__name__)

    def error(self: Log4j, message: str) -> None:
        """Log an error.

        Args:
            message (str): Error message to write to log
        """
        self.logger.error(message)

    def warn(self: Log4j, message: str) -> None:
        """Log a warning.

        Args:
            message (str): Warning messsage to write to log
        """
        self.logger.warning(message)

    def info(self: Log4j, message: str) -> None:
        """Log information.

        Args:
            message (str): Information message to write to log
        """
        self.logger.info(message)
