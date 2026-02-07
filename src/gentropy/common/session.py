"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from urllib.request import urlopen

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class NativeFileFormat(StrEnum):
    """Enum for supported file formats."""

    PARQUET = "parquet"
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"

    @classmethod
    def uri_parallelizable(cls) -> list[str]:
        """Get the list of file formats that can be parallelized when loading from a URI.

        Returns:
            list[str]: List of file formats that can be parallelized when loading from a URI.
        """
        return [NativeFileFormat.CSV.value, NativeFileFormat.TSV.value]


class SparkWriteMode(StrEnum):
    """Enum for Spark write modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR_IF_EXISTS = "errorifexists"


class Session:
    """This class provides a wrapper around SparkSession object with custom parameters.

    The wrapper has a few default sets of configurations. See constructor for references.

    !!! info "Custom Spark Configuration"
        -  **Output configuration**: write_mode and output_partitions, these set of parameters is stored respectively
            under `spark.gentropy.writeMode` and `spark.gentropy.outputPartitions`.
            Both parameters are used when writing datasets in gentropy steps. The `writeMode` will reflect on how Spark should handle existing data at the output path,
            while `outputPartitions` will determine the number of partitions to use when writing out datasets (typically, excluding studyIndex datasets). For exact usage check the respective step implementation.
        - **Hail configuration**: If `start_hail` is set to True, the Spark session will be configured with hail.
            By default the path to the Hail jar will be inferred from the installed Hail package location.
            Note that custom Hail configuration parameters can be passed through the `extended_hail_conf` argument.
        - **Dynamic allocation configuration**: If `dynamic_allocation` is set to True, the Spark session will include
            `spark.dynamicAllocation.enabled`, `spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.initialExecutors` and `spark.shuffle.service.enabled` configurations with 2 executors as minimum.
        - **Enhanced BGZF codec configuration**: If `use_enhanced_bgzip_codec` is set to True, the Spark session will be configured to use the `BGZFEnhancedGzipCodec` for reading block gzipped files.

    Note:
        The custom configuration parameters for gentropy are prefixed with `spark.gentropy.` to avoid conflicts with other Spark applications.

    Examples:
        Create a new Spark Session on local machine with 4 executors, 4 cores and 8g of memory per executor

        >>> from gentropy.common.session import Session
        >>> session = Session(
        ...     spark_uri="local[4]",
        ...     extended_spark_conf={
        ...         "spark.executor.instances": "4",
        ...         "spark.executor.cores": "4",
        ...         "spark.executor.memory": "8g",
        ...     },
        ... ) # doctest: +SKIP

        Find existing session (if any exists)

        >>> session = Session.find() # doctest: +SKIP

        Create a new Spark Session with Hail support

        >>> session = Session(start_hail=True) # doctest: +SKIP

        Connect to running Spark cluster (yarn)

        >>> session = Session(spark_uri="yarn") # doctest: +SKIP

        Specify custom Hail configuration parameters

        >>> session = Session(
        ...     start_hail=True,
        ...     extended_hail_conf={"min_block_size": "32MB"}
        ... ) # doctest: +SKIP

        Specify custom output parameters

        >>> session = Session(
        ...     output_partitions=100,
        ...     write_mode="overwrite"
        ... ) # doctest: +SKIP

        Stop the session

        >>> session.spark.stop() # doctest: +SKIP

        View the path to spark ui

        >>> session.spark.sparkContext.uiWebUrl # doctest: +SKIP

    """

    def __init__(
        self: Session,
        spark_uri: str = "local[*]",
        app_name: str = "gentropy",
        write_mode: SparkWriteMode = SparkWriteMode.ERROR_IF_EXISTS,
        hail_home: str | None = None,
        start_hail: bool = False,
        extended_spark_conf: dict[str, str] | None = None,
        extended_hail_conf: dict[str, str | int | bool] | None = None,
        output_partitions: int = 200,
        use_enhanced_bgzip_codec: bool = False,
        dynamic_allocation: bool = True,
    ) -> None:
        """Initialises spark session and logger.

        The wrapper over SparkSession will either connect to an existing active Spark session or create a new one with the provided configuration.

        Args:
            spark_uri (str): Spark URI. Defaults to "local[*]".
            app_name (str): Spark application name. Defaults to "gentropy".
            write_mode (SparkWriteMode): Spark write mode. Defaults to SparkWriteMode.ERROR_IF_EXISTS.
            hail_home (str | None): Path to Hail installation. Defaults to None.
            start_hail (bool): Whether to start Hail. Defaults to False.
            extended_spark_conf (dict[str, str] | None): Extended Spark configuration. Defaults to None.
            extended_hail_conf (dict[str, str | int | bool] | None): Extended Hail configuration. Defaults to None.
            output_partitions (int): Number of partitions for output datasets. Defaults to 200.
            use_enhanced_bgzip_codec (bool): Whether to use the BGZFEnhancedGzipCodec for reading block gzipped files. Defaults to False.
            dynamic_allocation (bool): Whether to enable Spark dynamic allocation. Defaults to True.

        """
        # Provide sane defaults for extended configurations
        extended_hail_conf = extended_hail_conf or {}
        extended_spark_conf = extended_spark_conf or {}
        write_mode = write_mode or SparkWriteMode.ERROR_IF_EXISTS
        output_partitions = output_partitions or 200

        # Create a fresh SparkConf object...
        _c = SparkConf(loadDefaults=False)
        # ...and update it with requested parameters
        _c = self._setup_output_config(_c, output_partitions, write_mode)
        _c = self._setup_log4j_config(_c)
        if dynamic_allocation:
            _c = self._setup_dynamic_allocation_config(_c)
        if start_hail:
            _c = self._setup_hail_config(_c, hail_home)
        if use_enhanced_bgzip_codec:
            _c = self._setup_enhanced_bgzip_config(_c)
        if extended_spark_conf:
            # If any additional packages or jars, ensure they are included along existing ones instead of overwritten
            _c = self._setup_extended_spark_conf(extended_spark_conf, _c)
        # Create or retrieve the Spark session
        # if the session does not exist yet, the new configuration will be used ...
        _spark_exists = isinstance(SparkSession.getActiveSession(), SparkSession)
        spark = (
            SparkSession.Builder()
            .config(conf=_c)
            .master(spark_uri)
            .appName(app_name)
            .getOrCreate()
        )
        self.logger = Log4j(spark)
        self.spark = spark
        # ...otherwise set the revitalized configuration to the existing session
        # NOTE: this will only work for certain parameters that can be set at runtime
        if _spark_exists:
            self._update_runtime_conf(spark, _c)

        # Initialize Hail if requested
        if start_hail:
            import hail as hl

            extended_hail_conf.setdefault("log", "/dev/null")
            extended_hail_conf.setdefault("quiet", True)
            extended_hail_conf.setdefault("idempotent", True)
            hl.init(sc=spark.sparkContext, **extended_hail_conf)

        self.conf = spark.sparkContext.getConf()

    @property
    def output_partitions(self) -> int:
        """Get the number of output partitions.

        Returns:
            int: Number of output partitions.
        """
        return int(self.conf.get("spark.gentropy.outputPartitions", "200"))

    @property
    def write_mode(self) -> SparkWriteMode:
        """Get the Spark write mode.

        Returns:
            SparkWriteMode: Spark write mode.
        """
        return SparkWriteMode(
            self.conf.get(
                "spark.gentropy.writeMode", SparkWriteMode.ERROR_IF_EXISTS.value
            )
        )

    @classmethod
    def find(cls) -> Session:
        """Finds the current active Spark session.

        If no active Spark session is found, the method will raise an AttributeError.

        Returns:
            Session: Current active Spark session.

        Raises:
            AttributeError: If no active Spark session is found.
        """
        active_spark = SparkSession.getActiveSession()
        if active_spark is None:
            raise AttributeError("Active Spark not found.")
        return Session()

    @classmethod
    def _setup_extended_spark_conf(
        cls, extended_spark_conf: dict[str, str], _c: SparkConf
    ) -> SparkConf:
        """Append extended spark configuration to the existing SparkConf object.

        This method ensures that packages and jars are included instead of overwritten.

        Args:
            extended_spark_conf (dict[str, str]): Extended Spark configuration to include in the session.
            _c (SparkConf): Existing SparkConf object to update.

        Returns:
            SparkConf: Updated SparkConf object with extended configuration included.
        """
        for key, value in extended_spark_conf.items():
            if key == "spark.jars":
                _c = Session._append_jar(_c, value)
            if key == "spark.jars.packages":
                _c = Session._append_package(_c, value)
            if key == "spark.driver.extraClassPath":
                _c = Session._append_to_driver_classpath(_c, value)
            if key == "spark.executor.extraClassPath":
                _c = Session._append_to_executor_classpath(_c, value)
            else:
                _c = _c.set(key, value)
        return _c

    @staticmethod
    def _setup_output_config(
        c: SparkConf, output_partitions: int, write_mode: SparkWriteMode
    ) -> SparkConf:
        """Output spark configuration.

        Args:
            c (SparkConf): Existing Spark configuration.
            output_partitions (int): Number of output partitions.
            write_mode (SparkWriteMode): Spark write mode.

        Returns:
            SparkConf: adjusted spark configuration with output settings.
        """
        return c.set("spark.gentropy.outputPartitions", str(output_partitions)).set(
            "spark.gentropy.writeMode", write_mode
        )

    @staticmethod
    def _setup_dynamic_allocation_config(c: SparkConf) -> SparkConf:
        """Setup Spark dynamic allocation configuration.

        Args:
            c (SparkConf): Existing Spark configuration.

        Returns:
            SparkConf: Adjusted spark configuration with dynamic allocation settings.
        """
        return (
            c.set("spark.dynamicAllocation.enabled", "true")
            .set("spark.dynamicAllocation.minExecutors", "2")
            .set("spark.dynamicAllocation.initialExecutors", "2")
            .set("spark.shuffle.service.enabled", "true")
        )

    @staticmethod
    def _setup_hail_config(
        c: SparkConf,
        hail_home: str | None = None,
    ) -> SparkConf:
        """Setup Hail Spark configuration.

        Args:
            c (SparkConf): Existing Spark configuration.
            hail_home (str | None): Path to Hail installation.

        Returns:
            SparkConf: Adjusted spark configuration with Hail settings.
        """
        if not hail_home:
            import hail as hl

            hail_home = Path(hl.__file__).parent.as_posix()
        jar_path = f"{hail_home}/backend/hail-all-spark.jar"
        if not Path(jar_path).exists():
            raise FileNotFoundError(
                f"Hail jar not found at {jar_path}. Please set hail_home in Session."
            )
        c = Session._append_jar(c, jar_path)
        c = Session._append_to_driver_classpath(c, jar_path)
        c = Session._append_to_executor_classpath(c, "./hail-all-spark.jar")
        return (
            c.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.kryo.registrator", "is.hail.kryo.HailKryoRegistrator")
            .set("spark.gentropy.enableHail", "true")
            .set("spark.gentropy.hailHome", hail_home)
        )

    @staticmethod
    def _setup_enhanced_bgzip_config(c: SparkConf) -> SparkConf:
        """Spark configuration for reading block gzipped files.

        Args:
            c (SparkConf): Existing Spark configuration.

        Returns:
            SparkConf: Adjusted spark configuration with BGZFEnhancedGzipCodec settings.

        Configuration that adds the hadoop-bam package and sets the BGZFEnhancedGzipCodec.
        Based on hadoop-bam jar artifact from [maven](https://mvnrepository.com/artifact/org.seqdoop/hadoop-bam/7.10.0).

        Note:
            Full details of the codec can be found in [hadoop-bam](https://github.com/HadoopGenomics/Hadoop-BAM/blob/7.10.0/src/main/java/org/seqdoop/hadoop_bam/util/BGZFEnhancedGzipCodec.java)

        This codec implements:
          (1) SplittableCompressionCodec allowing parallel reading of bgzip files.
          (2) GzipCodec allowing reading of standard gzip files.
        """
        c = Session._append_package(c, "org.seqdoop:hadoop-bam:7.10.0")
        return c.set(
            "spark.hadoop.io.compression.codecs",
            "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec",
        ).set("spark.gentropy.useEnhancedBgzipCodec", "true")

    @staticmethod
    def _append_jar(c: SparkConf, jar: str) -> SparkConf:
        """Append a jar to the existing spark.jars configuration.

        Args:
            c (SparkConf): Existing Spark configuration.
            jar (str): Jar to add to the configuration.

        Returns:
            SparkConf: Adjusted spark configuration with the new jar included in the spark.jars setting.
        """
        existing_jars = c.get("spark.jars", "")
        if jar not in existing_jars:
            new_jars = f"{existing_jars},{jar}" if existing_jars else jar
            return c.set("spark.jars", new_jars)
        return c

    @staticmethod
    def _append_package(c: SparkConf, package: str) -> SparkConf:
        """Append a package to the existing spark.jars.packages configuration.

        Args:
            c (SparkConf): Existing Spark configuration.
            package (str): Package to add to the configuration.

        Returns:
            SparkConf: Adjusted spark configuration with the new package included in the spark.jars.packages setting.
        """
        existing_packages = c.get("spark.jars.packages", "")
        if package not in existing_packages:
            new_packages = (
                f"{existing_packages},{package}" if existing_packages else package
            )
            return c.set("spark.jars.packages", new_packages)
        return c

    @staticmethod
    def _append_to_executor_classpath(c: SparkConf, jar: str) -> SparkConf:
        """Append a jar to the existing driver and executor classpath.

        Args:
            c (SparkConf): Existing Spark configuration.
            jar (str): Jar to add to the classpath.

        Returns:
            SparkConf: Adjusted spark configuration with the new jar included in the driver and executor classpath.
        """
        existing_executor_cp = c.get("spark.executor.extraClassPath", "")
        new_executor_cp = (
            f"{existing_executor_cp},{jar}" if existing_executor_cp else jar
        )
        if jar not in existing_executor_cp:
            return c.set("spark.executor.extraClassPath", new_executor_cp)
        return c

    @staticmethod
    def _append_to_driver_classpath(c: SparkConf, jar: str) -> SparkConf:
        """Append a jar to the existing driver classpath.

        Args:
            c (SparkConf): Existing Spark configuration.
            jar (str): Jar to add to the classpath.

        Returns:
            SparkConf: Adjusted spark configuration with the new jar included in the driver classpath.
        """
        existing_driver_cp = c.get("spark.driver.extraClassPath", "")
        new_driver_cp = f"{existing_driver_cp},{jar}" if existing_driver_cp else jar
        if jar not in existing_driver_cp:
            return c.set("spark.driver.extraClassPath", new_driver_cp)
        return c

    @staticmethod
    def _setup_log4j_config(c: SparkConf) -> SparkConf:
        """Setup Log4j Spark configuration.

        Args:
            c (SparkConf): Existing Spark configuration.

        Returns:
            SparkConf: Adjusted spark configuration with log4j settings.

        !!! info "Log4j Configuration":
            This method points to the static log4j properties file included in the gentropy assets.
            The default configuration sets the log level to ERROR for all Spark logs. This is done to
            prevent the excessive logging from Spark initialization, the actual log level can be adjusted
            post initialization using the Log4j class.
        """
        import importlib.resources as pkg_resources

        from gentropy import assets as asf

        prop = str(pkg_resources.files(asf).joinpath("log4j.properties"))
        c.set("spark.driver.extraJavaOptions", f"-Dlog4j.configuration=file:{prop}")
        return c

    def _update_runtime_conf(self, spark: SparkSession, c: SparkConf) -> None:
        """Update runtime Spark configuration.

        This method will attempt to modify in place SparkSession.conf with the provided SparkConf.

        Args:
            spark (SparkSession): Spark session to update.
            c (SparkConf): Spark configuration with desired settings.

        """
        for key, value in c.getAll():
            if not spark.conf.isModifiable(key) or key.startswith("spark.gentropy."):
                self.logger.warning(
                    f"Spark configuration '{key}' is not modifiable at runtime and will be skipped."
                )
                continue
            spark.conf.set(key, value)

    def load_data(
        self: Session,
        path: str | list[str],
        fmt: str,
        schema: StructType | str | None = None,
        **kwargs: bool | float | int | str | None,
    ) -> DataFrame:
        """Generic function to read a file or folder into a Spark dataframe.

        The `recursiveFileLookup` flag when set to True will skip all partition columns, but read files from all subdirectories.

        Args:
            path (str | list[str]): path to the dataset
            fmt (str): file format. Defaults to parquet.
            schema (StructType | str | None): Schema to use when reading the data.
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.load. `mergeSchema` is set to True, `recursiveFileLookup` is set to False by default.

        Returns:
            DataFrame: Dataframe containing the loaded data.
        """
        # Set default kwargs
        _format = fmt.lower()
        match _format:
            case "parquet":
                _fmt = NativeFileFormat.PARQUET.value
            case "tsv":
                _fmt = NativeFileFormat.CSV.value
                kwargs.setdefault("sep", "\t")
                kwargs.setdefault("header", True)
            case "csv":
                _fmt = NativeFileFormat.CSV.value
                kwargs.setdefault("header", True)
            case "json" | "jsonl" | "jsonlines":
                _fmt = NativeFileFormat.JSON.value
            case _:
                raise ValueError(f"Unsupported file format: {format}")

        match path:
            case list():
                all_strings = len(path) > 0 and all(isinstance(p, str) for p in path)
                assert all_strings, "Path must be a non-empty list of strings."
            case str():
                if path.startswith(("http://", "https://")):
                    return self._load_from_url(path, fmt=_fmt, schema=schema, **kwargs)  # type: ignore[arg-type]
            case _:
                raise ValueError("Path must be a string or a list of strings.")

        return self.spark.read.load(path, format=_fmt, schema=schema, **kwargs)

    def _load_from_url(
        self: Session,
        url: str,
        fmt: str,
        schema: StructType | str | None = None,
        **kwargs: str,
    ) -> DataFrame:
        """Load CSV/TSV data from a URL into a Spark DataFrame.

        Args:
            url (str): URL or list of URLs to load data from.
            fmt (str): File format. Currently only 'csv' is supported.
            schema (StructType | str | None): Schema to use when reading the data.
            **kwargs (str): Additional arguments to pass to spark.read.csv.

        Returns:
            DataFrame: Dataframe containing the loaded data.
        """
        self.logger.warning(
            "Reading data over HTTP/HTTPS. This may be slow for large datasets. Consider downloading the data to a distributed file system."
        )

        assert fmt in NativeFileFormat.uri_parallelizable(), (
            "Only 'csv' and 'tsv' formats are supported for loading data from URL."
        )
        if not schema:
            kwargs.setdefault("inferSchema", "true")

        with urlopen(url) as response:
            csv_data = response.read().decode("utf8").splitlines()
            rdd = self.spark.sparkContext.parallelize(csv_data)
            return self.spark.read.csv(rdd, schema=schema, **kwargs)


class JavaLogger(Protocol):
    """Protocol for Java Log4j Logger accessed through PySpark JVM bridge."""

    def error(self, message: str) -> None:
        """Log an error message.

        Args:
            message (str): The error message to log.
        """

    def warn(self, message: str) -> None:
        """Log a warning message.

        Args:
            message (str): The error message to log.
        """

    def info(self, message: str) -> None:
        """Log an info message.

        Args:
            message (str): The error message to log.
        """


class Log4j:
    """Log4j logger class."""

    def __init__(self, spark: SparkSession, level: str = "ERROR") -> None:
        """Log4j logger class. This class provides a wrapper around the Log4j logging system.

        Args:
            spark (SparkSession): The Spark session used to access Spark context and Log4j logging.
            level (str): Logging level. Defaults to "ERROR".
        """
        log4j: Any = spark.sparkContext._jvm.org.apache.log4j  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
        spark.sparkContext.setLogLevel(level)
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
