"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

import hail as hl
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class Session:
    """This class provides a Spark session and logger."""

    def __init__(
        self: Session,
        spark_uri: str = "local[*]",
        write_mode: str = "errorifexists",
        app_name: str = "gentropy",
        hail_home: str | None = None,
        start_hail: bool = False,
        extended_spark_conf: dict[str, str] | None = None,
        output_partitions: int = 200,
        use_enhanced_bgzip_codec: bool = False,
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
            use_enhanced_bgzip_codec (bool): Whether to use the BGZFEnhancedGzipCodec for reading block gzipped files. Defaults to False.
        """
        merged_conf = self._create_merged_config(
            start_hail,
            use_enhanced_bgzip_codec,
            extended_spark_conf,
            hail_home,
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
        self.use_enhanced_bgzip_codec = use_enhanced_bgzip_codec
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
            .set("spark.shuffle.service.enabled", "true")
        )

    def _bgzip_config(self: Session) -> SparkConf:
        """Spark configuration for reading block gzipped files.

        Configuration that adds the hadoop-bam package and sets the BGZFEnhancedGzipCodec.
        Based on hadoop-bam jar artifact from [maven](https://mvnrepository.com/artifact/org.seqdoop/hadoop-bam/7.10.0).

        Note:
            Full details of the codec can be found in [hadoop-bam](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.10.0/src/main/java/org/seqdoop/hadoop_bam/util/BGZFEnhancedGzipCodec.java)

        This codec implements:
        (1) SplittableCompressionCodec allowing parallel reading of bgzip files.
        (2) GzipCodec allowing reading of standard gzip files.

        Returns:
            SparkConf: Spark configuration for reading block gzipped files.
        """
        return (
            SparkConf()
            .set("spark.jars.packages", "org.seqdoop:hadoop-bam:7.10.0")
            .set(
                "spark.hadoop.io.compression.codecs",
                "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec",
            )
        )

    def _hail_config(self: Session, hail_home: str) -> SparkConf:
        """Returns the Hail specific Spark configuration.

        Args:
            hail_home (str): Path to Hail installation.

        Returns:
            SparkConf: Hail specific Spark configuration.
        """
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
        use_enhanced_bgzip_codec: bool,
        extended_spark_conf: dict[str, str] | None,
        hail_home: str | None = None,
    ) -> SparkConf:
        """Merges the default, and optionally the Hail and extended configurations if provided.

        Args:
            start_hail (bool): Whether to start Hail.
            use_enhanced_bgzip_codec (bool): Whether to use the BGZFEnhancedGzipCodec for reading block gzipped files.
            extended_spark_conf (dict[str, str] | None): Extended Spark configuration.
            hail_home (str | None): Path to Hail installation.

        Raises:
            ValueError: If Hail home is not specified but Hail is requested.

        Returns:
            SparkConf: Merged Spark configuration.
        """
        all_settings = self._default_config().getAll()
        if start_hail:
            if not hail_home:
                raise ValueError("Hail home must be specified to start Hail.")
            all_settings += self._hail_config(hail_home).getAll()
        if use_enhanced_bgzip_codec:
            all_settings += self._bgzip_config().getAll()
        if extended_spark_conf is not None:
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


class JavaLogger(Protocol):
    """Protocol for Java Log4j Logger accessed through PySpark JVM bridge."""

    def error(self, message: str) -> None:
        """Log an error message.

        Args:
            message (str): The error message to log.
        """
        ...

    def warn(self, message: str) -> None:
        """Log a warning message.

        Args:
            message (str): The error message to log.
        """
        ...

    def info(self, message: str) -> None:
        """Log an info message.

        Args:
            message (str): The error message to log.
        """
        ...


class Log4j:
    """Log4j logger class."""

    def __init__(self, spark: SparkSession) -> None:
        """Log4j logger class. This class provides a wrapper around the Log4j logging system.

        Args:
            spark (SparkSession): The Spark session used to access Spark context and Log4j logging.
        """
        log4j: Any = spark.sparkContext._jvm.org.apache.log4j  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
        # Cast to our protocol type for type safety
        self.logger: JavaLogger = log4j.LogManager.getLogger(__name__)

    def error(self, message: str) -> None:
        """Log an error.

        Args:
            message (str): Error message to write to log
        """
        self.logger.error(message)

    def warning(self, message: str) -> None:
        """Log a warning.

        Args:
            message (str): Warning message to write to log
        """
        self.logger.warn(message)  # noqa: G010

    def info(self, message: str) -> None:
        """Log information.

        Args:
            message (str): Information message to write to log
        """
        self.logger.info(message)
