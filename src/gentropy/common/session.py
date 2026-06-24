"""Classes to reuse spark connection and logging functionalities."""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import pandas as pd
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


class SparkWriteMode(StrEnum):
    """Enum for Spark write modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR_IF_EXISTS = "errorifexists"

    @classmethod
    def ensure(cls, v: str | None) -> str:
        """Ensure the writeMode is correct.

        Args:
            v (str | None): input value

        Returns:
            str: mapping

        Raises:
            ValueError: when the value is not found.
        """
        match v:
            case "append":
                return cls.APPEND.value
            case "overwrite":
                return cls.OVERWRITE.value
            case "ignore":
                return cls.IGNORE.value
            case "errorifexists" | None:
                return cls.ERROR_IF_EXISTS.value
            case _:
                raise ValueError("Incorrect writeMode specified to Session object.")


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
        ...     write_mode=SparkWriteMode.OVERWRITE
        ... ) # doctest: +SKIP

        Specify via string (auto-converted to SparkWriteMode) if possible

        >>> session = Session(
        ...     output_partitions=100,
        ...     write_mode="overwrite"
        ... ) # doctest: +SKIP

        Stop the session

        >>> session.spark.stop() # doctest: +SKIP

        View the path to spark ui

        >>> session.spark.sparkContext.uiWebUrl # doctest: +SKIP

        Example session with hadoop connector for S3 compatible storage

        Credentials can be passed directly via ``s3_configuration``, or loaded
        from the environment (see :class:`~gentropy.external.s3.S3Config`).

        >>> session = Session(
        ...     add_s3_connector=True,
        ...     s3_configuration={
        ...         'bucket_name': 'my-bucket',
        ...         's3_host_url': 's3.my-domain.com',
        ...         's3_host_port': 9000,
        ...         'access_key_id': 'my-access-key',
        ...         'secret_access_key': 'my-secret-key',
        ...     },
        ...     extended_spark_conf={
        ...         # Executor
        ...         'spark.executor.memory': '32g',
        ...         'spark.executor.cores': '8',
        ...         'spark.executor.memoryOverhead': '4g',
        ...         'spark.sql.files.maxPartitionBytes': '512m',
        ...         # Driver
        ...         'spark.driver.memory': '25g',
        ...         'spark.executor.extraJavaOptions': '-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch',
        ...         # Throughput tuning
        ...         'spark.hadoop.fs.s3a.connection.maximum': '1000',
        ...         'spark.hadoop.fs.s3a.threads.max': '1024',
        ...         'spark.hadoop.fs.s3a.attempts.maximum': '20',
        ...         'spark.hadoop.fs.s3a.connection.timeout': '600000',  # 10min
        ...     },
        ... ) # doctest: +SKIP

        Or relying on environment variables (``AWS_S3_BUCKET_NAME``, ``AWS_ENDPOINT_URL``,
        ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``):

        >>> session = Session(add_s3_connector=True) # doctest: +SKIP

        Example session with hadoop connector for Google Cloud Storage

        Uses the VM's attached service account (``COMPUTE_ENGINE`` auth) by default — the
        natural authentication mode on Dataproc. ``project_id`` is optional and only needed
        for bucket-level operations or as a requester-pays billing project fallback.
        See :class:`~gentropy.external.gcs.GCSConfig` for the full set of options.

        To allow running locally with GCS credentials, one can set the ``auth_type``
        to ``APPLICATION_DEFAULT`` and ensure that ``gcloud auth application-default login``
        has been run to populate the local ADC credentials file.

        >>> session = Session(
        ...     add_gcs_connector=True,
        ...     gcs_configuration={'project_id': 'my-gcp-project'},
        ...     extended_spark_conf={
        ...        'spark.driver.maxResultSize': '0',
        ...        'spark.sql.broadcastTimeout': '3000',
        ...        'spark.sql.adaptive.enabled': 'true',
        ...        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        ...        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        ...        'spark.hadoop.fs.gs.block.size': '134217728',
        ...        'spark.hadoop.fs.gs.outputstream.buffer.size': '8388608',
        ...        'spark.hadoop.fs.gs.outputstream.sync.min.interval': '2s',
        ...     },
        ... ) # doctest: +SKIP

        Cross-project access via service account impersonation:

        >>> session = Session(
        ...     add_gcs_connector=True,
        ...     gcs_configuration={
        ...         'impersonation_sa': 'target-sa@other-project.iam.gserviceaccount.com',
        ...         'requester_pays': 'ENABLED',
        ...         'requester_pays_project_id': 'billing-project',
        ...     },
        ... ) # doctest: +SKIP

        Or relying on environment variables (``GCS_PROJECT_ID``, ``GCS_AUTH_TYPE``,
        ``GCS_IMPERSONATION_SA``, ``GCS_REQUESTER_PAYS``, etc.):

        >>> session = Session(add_gcs_connector=True) # doctest: +SKIP

    """

    def __init__(
        self: Session,
        spark_uri: str = "local[*]",
        app_name: str = "gentropy",
        *,
        write_mode: str = SparkWriteMode.ERROR_IF_EXISTS.value,
        hail_home: str | None = None,
        start_hail: bool = False,
        extended_spark_conf: dict[str, str] | None = None,
        extended_hail_conf: dict[str, Any] | None = None,
        output_partitions: int = 200,
        use_enhanced_bgzip_codec: bool = False,
        dynamic_allocation: bool = True,
        log_level: str | None = "INFO",
        add_s3_connector: bool = False,
        add_gcs_connector: bool = False,
        s3_configuration: dict[str, Any] | None = None,
        gcs_configuration: dict[str, Any] | None = None,
        s3_configuration_path: str | None = None,
        gcs_configuration_path: str | None = None,
    ) -> None:
        """Initialises spark session and logger.

        The wrapper over SparkSession will either connect to an existing active Spark session or create a new one with the provided configuration.

        If spark session already exists, the provided configuration will have no effect on the session.
        If any parameters will be different between existing session config and requested config,
          a warning will be logged to suggest rebuilding the session with the new configuration.

        Args:
            spark_uri (str): Spark URI. Defaults to "local[*]".
            app_name (str): Spark application name. Defaults to "gentropy".
            write_mode (str): Spark write mode. Defaults to SparkWriteMode.ERROR_IF_EXISTS.
            hail_home (str | None): Path to Hail installation. Defaults to None.
            start_hail (bool): Whether to start Hail. Defaults to False.
            extended_spark_conf (dict[str, str] | None): Extended Spark configuration. Defaults to None.
            extended_hail_conf (dict[str, Any] | None): Extended Hail configuration. Defaults to None.
            output_partitions (int): Number of partitions for output datasets. Defaults to 200.
            use_enhanced_bgzip_codec (bool): Whether to use the BGZFEnhancedGzipCodec for reading block gzipped files. Defaults to False.
            dynamic_allocation (bool): Whether to enable Spark dynamic allocation. Defaults to True.
            log_level (str | None): Spark log level. Defaults to "INFO".
            add_s3_connector (bool): Whether to setup Spark configuration for S3 compatible storage. Defaults to False.
                If specified as `true`, the session will request the AWS S3 connector and attempt to resolve
                necessary credentials (see `gentropy.external.s3.S3Config`) from the provided `s3_configuration` parameter, `s3_configuration_path` or environment variables
                in that order of precedence. If no credentials are provided, the failure is raised.
            add_gcs_connector (bool): Whether to setup Spark configuration for GCS compatible storage. Defaults to False.
                If specified as `true`, the session will request the GCS connector and attempt to resolve
                necessary credentials (see `gentropy.external.gcs.GCSConfig`) from the provided `gcs_configuration` parameter, `gcs_configuration_path` or environment variables
                in that order of precedence. If no credentials are provided, the failure is raised.
            s3_configuration (dict[str, Any] | None): Optional dictionary with s3 configuration parameters to include in the session. Defaults to None.
                The object needs to follow the `gentropy.external.s3.S3Config` class.
            gcs_configuration (dict[str, Any] | None): Optional dictionary with GCS configuration parameters to include in the session. Defaults to None.
                The object needs to follow the `gentropy.external.gcs.GCSConfig` class.
            s3_configuration_path (str | None): Optional path to a JSON file with S3 configuration parameters. Defaults to None. If file is provided,
                it will need to live in `shared` folder that is accessible by the Spark cluster (not in the distributed file system.)
            gcs_configuration_path (str | None): Optional path to a JSON file with GCS configuration parameters. Defaults to None. If file is provided,
                it will need to live in `shared` folder that is accessible by the Spark cluster (not in the distributed file system.)
        """
        # Provide sane defaults for extended configurations

        self._extended_hail_conf = extended_hail_conf or {}
        self._extended_spark_conf = extended_spark_conf or {}
        self._write_mode = SparkWriteMode.ensure(write_mode)
        self._output_partitions = output_partitions or 200
        self._hail_home = hail_home
        self._s3_configuration = s3_configuration or {}
        self._s3_configuration_path = s3_configuration_path
        self._gcs_configuration = gcs_configuration or {}
        self._gcs_configuration_path = gcs_configuration_path
        self._add_s3_connector = add_s3_connector
        self._add_gcs_connector = add_gcs_connector
        # Build the requested config, small overhead, but we
        # can report if existing session is up to date with provided configuration.
        _c = self._build_config(
            dynamic_allocation=dynamic_allocation,
            start_hail=start_hail,
            use_enhanced_bgzip_codec=use_enhanced_bgzip_codec,
            add_s3_connector=add_s3_connector,
            add_gcs_connector=add_gcs_connector,
        )
        # Create or retrieve the Spark session
        _spark_exists = isinstance(SparkSession.getActiveSession(), SparkSession)
        if _spark_exists:
            self.spark = (
                SparkSession.Builder().master(spark_uri).appName(app_name).getOrCreate()
            )
            self.logger = Log4j(self.spark, level=log_level)
            self.conf = self.spark.sparkContext.getConf()
            # Check existing configuration against requested
            self._compare_conf(current=self.conf, requested=_c)
        else:
            # The sparkSession does not exist yet, initialize the spark session with new configuration
            self.spark = (
                SparkSession.Builder()
                .config(conf=_c)
                .master(spark_uri)
                .appName(app_name)
                .getOrCreate()
            )
            # Initialize Hail if requested
            if start_hail:
                from gentropy.common.imports import optional_imports

                with optional_imports("hail"):
                    import hail as hl

                self._extended_hail_conf.setdefault("log", "/dev/null")
                self._extended_hail_conf.setdefault("quiet", True)
                self._extended_hail_conf.setdefault("idempotent", True)
                hl.init(sc=self.spark.sparkContext, **self._extended_hail_conf)

            self.logger = Log4j(self.spark, level=log_level)
            self.conf = self.spark.sparkContext.getConf()

    def _build_config(
        self,
        dynamic_allocation: bool,
        start_hail: bool,
        use_enhanced_bgzip_codec: bool,
        add_s3_connector: bool,
        add_gcs_connector: bool,
    ) -> SparkConf:
        """Prepare the SparkConf object with the requested configuration.

        Args:
            dynamic_allocation (bool): Whether to enable Spark dynamic allocation.
            start_hail (bool): Whether to include Hail configuration.
            use_enhanced_bgzip_codec (bool): Whether to include enhanced BGZIP codec configuration.
            add_s3_connector (bool): Whether to include S3 connector configuration.
            add_gcs_connector (bool): Whether to include GCS connector configuration.

        Returns:
            SparkConf: SparkConf object with the requested configuration.

        """
        # Create a fresh SparkConf object...
        _c = SparkConf(loadDefaults=False)
        # ...and update it with requested parameters
        _c = self._setup_output_config(_c, self._output_partitions, self._write_mode)
        _c = self._setup_log4j_config(_c)
        if dynamic_allocation:
            _c = self._setup_dynamic_allocation_config(_c)
        if start_hail:
            _c = self._setup_hail_config(_c, self._hail_home)
        if use_enhanced_bgzip_codec:
            _c = self._setup_enhanced_bgzip_config(_c)
        if add_s3_connector:
            _c = self._setup_s3_connector(
                _c, self._s3_configuration or self._s3_configuration_path
            )
        if add_gcs_connector:
            _c = self._setup_gcs_connector(
                _c, self._gcs_configuration or self._gcs_configuration_path
            )
        # If any additional packages or jars, ensure they are included along existing ones instead of overwritten
        if self._extended_spark_conf:
            _c = self._setup_extended_spark_conf(self._extended_spark_conf, _c)
        return _c

    def _compare_conf(self, current: SparkConf, requested: SparkConf) -> None:
        """Compare current Spark configuration with the requested configuration.

        This method will log a warning for each configuration key that is present in the requested configuration but has a different value in the current configuration.

        Args:
            current (SparkConf): Current Spark configuration.
            requested (SparkConf): Requested Spark configuration.
        """
        for key, value in requested.getAll():
            current_value = current.get(key, None)
            if current_value != value:
                self.logger.warning(
                    f"Consider rebuilding SparkSession to apply requested configuration: '{key}' has value '{current_value}' but '{value}' was requested."
                )

    @property
    def use_enhanced_bgzip_codec(self) -> bool:
        """Check if the session is configured to use the BGZFEnhancedGzipCodec for reading block gzipped files.

        Returns:
            bool: True if the session is configured to use the BGZFEnhancedGzipCodec, False otherwise.
        """
        return (
            self.conf.get("spark.gentropy.useEnhancedBgzipCodec", "false").lower()
            == "true"
        )

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
            match key:
                case "spark.jars":
                    _c = Session._append_jar(_c, value)
                case "spark.jars.packages":
                    _c = Session._append_package(_c, value)
                case "spark.driver.extraClassPath":
                    _c = Session._append_to_driver_classpath(_c, value)
                case "spark.executor.extraClassPath":
                    _c = Session._append_to_executor_classpath(_c, value)
                case _:
                    _c = _c.set(key, value)
        return _c

    @staticmethod
    def _setup_output_config(
        c: SparkConf, output_partitions: int, write_mode: str
    ) -> SparkConf:
        """Output spark configuration.

        Args:
            c (SparkConf): Existing Spark configuration.
            output_partitions (int): Number of output partitions.
            write_mode (str): Spark write mode.

        Returns:
            SparkConf: adjusted spark configuration with output settings.
        """
        return c.set("spark.gentropy.outputPartitions", str(output_partitions)).set(
            "spark.gentropy.writeMode", str(write_mode)
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
            from gentropy.common.imports import optional_imports

            with optional_imports("hail"):
                import hail as hl

            hail_home = Path(hl.__file__).parent.as_posix()
        jar_path = f"{hail_home}/backend/hail-all-spark.jar"
        if not Path(jar_path).exists():
            raise FileNotFoundError(
                f"Hail jar not found at {jar_path}. Please set hail_home in Session."
            )
        c = Session._append_jar(c, jar_path)
        c = Session._append_to_driver_classpath(c, jar_path)
        # NOTE: the docs mention to not use full path for exectuor classPath
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

    def _setup_s3_connector(
        self, c: SparkConf, s3_configuration: dict[str, Any] | str | None = None
    ) -> SparkConf:
        """Setup Spark configuration for S3 compatible storage.

        Args:
            c (SparkConf): Existing Spark configuration.
            s3_configuration (dict[str, Any] | str | None): Dictionary with s3 configuration or path to json containing
                 parameters to include in the session or path to configuration file.

        Returns:
            SparkConf: Adjusted spark configuration with the S3 settings included.

        !!! info "S3 Connector Configuration"
            Set up the credentials for S3 connector using following parameters in order of precedence:
            1. `s3_configuration` provided as a dictionary
            2. `s3_configuration` provided as a path to json file
            3. Environment variables (see `S3Config.from_env` for more details)

        """
        from gentropy.external.s3 import S3Config

        if s3_configuration is not None and not isinstance(s3_configuration, (dict, str)):
            s3_configuration = dict(s3_configuration)

        match s3_configuration:
            case dict():
                conf = S3Config(**s3_configuration)
            case str():
                conf = S3Config.from_json(s3_configuration)
            case _:
                conf = S3Config.from_env()
        data = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": f"https://{conf.s3_host_url}:{conf.s3_host_port}",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.access.key": conf.access_key_id.get_secret_value(),
            "spark.hadoop.fs.s3a.secret.key": conf.secret_access_key.get_secret_value(),
        }

        for key, value in data.items():
            c = c.set(key, value)

        c = self._append_package(c, S3Config._HADOOP_CONNECTOR_PKG)
        self._s3_configuration = dict(conf)
        return c

    def _setup_gcs_connector(
        self,
        c: SparkConf,
        gcs_configuration: dict[str, Any] | str | None = None,
    ) -> SparkConf:
        """Setup Spark configuration for GCS compatible storage.

        Args:
            c (SparkConf): Existing Spark configuration.
            gcs_configuration (dict[str, Any] | str | None): Dictionary with GCS configuration parameters to include in the session or path to json containing
                 parameters to include in the session or path to configuration file.

        Returns:
            SparkConf: Adjusted spark configuration with the GCS settings included.

        !!! info "GCS Connector Configuration"
            Set up the credentials for GCS connector using following parameters in order of precedence:
            1. `gcs_configuration` provided as a dictionary
            2. `gcs_configuration` provided as a path to json file
            3. Environment variables (see `GCSConfig.from_env` for more details)
        """
        from gentropy.external.gcs import GCSConfig

        # Hydra passes DictConfig (OmegaConf) objects which don't inherit from dict
        if gcs_configuration is not None and not isinstance(gcs_configuration, (dict, str)):
            gcs_configuration = dict(gcs_configuration)

        match gcs_configuration:
            case dict():
                conf = GCSConfig(**gcs_configuration)
            case str():
                conf = GCSConfig.from_json(gcs_configuration)
            case _:
                conf = GCSConfig.from_env()

        options: dict[str, str] = {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.auth.type": conf.auth_type.value,
            "spark.hadoop.fs.gs.status.parallel.enable": "true",
            "spark.hadoop.fs.gs.copy.with.rewrite.enable": "true",
            "spark.hadoop.fs.gs.glob.algorithm": "CONCURRENT",
            "spark.hadoop.fs.gs.requester.pays.mode": conf.requester_pays.value,
        }

        if conf.project_id is not None:
            options["spark.hadoop.fs.gs.project.id"] = conf.project_id
        if conf.keyfile_path is not None:
            options["spark.hadoop.fs.gs.auth.service.account.json.keyfile"] = (
                conf.keyfile_path
            )
        if conf.impersonation_sa is not None:
            options["spark.hadoop.fs.gs.auth.impersonation.service.account"] = (
                conf.impersonation_sa
            )
        if conf.requester_pays_project_id is not None:
            options["spark.hadoop.fs.gs.requester.pays.project.id"] = (
                conf.requester_pays_project_id
            )
        if conf.requester_pays_buckets is not None:
            options["spark.hadoop.fs.gs.requester.pays.buckets"] = ",".join(
                conf.requester_pays_buckets
            )

        for key, value in options.items():
            c = c.set(key, value)
        c = self._append_jar(c, GCSConfig._HADOOP_CONNECTOR_JAR)
        self._gcs_configuration = dict(conf)
        return c

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
    def _append_repository(c: SparkConf, repository: str) -> SparkConf:
        """Append a Maven repository URL to the existing spark.jars.repositories configuration.

        Ivy consults these repositories in addition to Maven Central when resolving
        packages declared via ``spark.jars.packages``.

        Args:
            c (SparkConf): Existing Spark configuration.
            repository (str): Maven repository URL to add (e.g. ``https://maven.google.com``).

        Returns:
            SparkConf: Adjusted spark configuration with the repository appended to
                ``spark.jars.repositories``.
        """
        existing = c.get("spark.jars.repositories", "")
        if repository not in existing:
            new_repos = f"{existing},{repository}" if existing else repository
            return c.set("spark.jars.repositories", new_repos)
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
        # NOTE: use os.pathsep, as it should default to ';' on windows and ':' on unix based systems.
        new_executor_cp = (
            f"{existing_executor_cp}{os.pathsep}{jar}" if existing_executor_cp else jar
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
        # NOTE: use os.pathsep, as it should default to ';' on windows and ':' on unix based systems.
        new_driver_cp = (
            f"{existing_driver_cp}{os.pathsep}{jar}" if existing_driver_cp else jar
        )
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

    def load_data(
        self: Session,
        path: str | list[str],
        fmt: str = "parquet",
        schema: StructType | str | None = None,
        **kwargs: bool | float | int | str | None,
    ) -> DataFrame:
        """Generic function to read a file or folder into a Spark dataframe.

        The `recursiveFileLookup` flag when set to True will skip all partition columns, but read files from all subdirectories.

        Args:
            path (str | list[str]): path to the dataset
            fmt (str): file format. Defaults to parquet.
            schema (StructType | str | None): Schema to use when reading the data.
            **kwargs (bool | float | int | str | None): Additional arguments to pass to spark.read.load.

        Returns:
            DataFrame: Dataframe containing the loaded data.

        !!! note "Default options for supported formats"
            By default:
            - `mergeSchema` is set to True for parquet format.
            - `recursiveFileLookup` is set to False.
            - For `tsv` format `sep` and `header` options are set to tab and `True` respectively.
            - For `csv` format `header` is set to `True`.

        !!! warning "Loading data from URL"
            If the provided path is a URL (starting with http:// or https://), the method will attempt to load the data
            and parallelize it for processing, this can be very slow it the file is large. Consider downloading the data
            to a distributed file system and loading it from there instead. Only supported formats for loading from URL are `csv` and `tsv`.
            Loading does not allow for recursive file lookup, nor supports multiple URLs.

        !!! note "Supported formats"
            Supported file formats are
            - parquet
            - csv
            - tsv
            - json (including jsonl/jsonlines)

        Examples:
            Load single tsv file from url, the header is expected at the 0-th row

            >>> session.load_data('https://some_file.tsv', fmt='tsv') # doctest: +SKIP

            Load single csv file from url, no header, expected schema

            >>> session.load_data('https://some_file.csv', fmt='csv', header=False, schema="A int, B int") # doctest: +SKIP

            Load the parquet dataset from google cloud storage, note that the Hadoop connector is required in Session

            >>> session.load_data('gs://your_bucket/dataset') # doctest: +SKIP

            Load multiple json files from s3 storage, note that the Hadoop connector is required in Session

            >>> session.load_data(['s3a://some_bucket/file1.jsonl', 's3a://some_bucket/file2.jsonl'], fmt='json') # doctest: +SKIP
        """
        # Set default kwargs
        _format = fmt.lower()
        kwargs.setdefault("recursiveFileLookup", False)

        match _format:
            case "parquet":
                _fmt = NativeFileFormat.PARQUET.value
                kwargs.setdefault("mergeSchema", True)
            case "tsv":
                _fmt = NativeFileFormat.CSV.value
                kwargs.setdefault("sep", "\t")
                kwargs.setdefault("header", True)
                if not schema:
                    kwargs.setdefault("inferSchema", "true")
            case "csv":
                _fmt = NativeFileFormat.CSV.value
                kwargs.setdefault("header", True)
                if not schema:
                    kwargs.setdefault("inferSchema", "true")
            case "json" | "jsonl" | "jsonlines":
                _fmt = NativeFileFormat.JSON.value
            case _:
                raise ValueError(f"Unsupported file format: {_format}")

        match path:
            case list():
                all_strings = len(path) > 0 and all(isinstance(p, str) for p in path)
                assert all_strings, "Path must be a non-empty list of strings."
            case str():
                if path.startswith(("http://", "https://")):
                    return self._load_from_url(path, fmt=_fmt, schema=schema, **kwargs)
            case _:
                raise ValueError("Path must be a string or a list of strings.")
        return self.spark.read.load(path, format=_fmt, schema=schema, **kwargs)

    def _load_from_url(
        self: Session,
        url: str,
        fmt: str,
        schema: StructType | str | None = None,
        **kwargs: Any,
    ) -> DataFrame:
        """Load CSV/TSV/JSON data from a URL into a Spark DataFrame.

        Args:
            url (str): single URL to load data from.
            fmt (str): File format. Currently only 'csv', 'tsv' or 'json' are supported for loading from URL.
            schema (StructType | str | None): Schema to use when reading the data.
            **kwargs (Any): Additional arguments to pass to spark.read.csv.

        Returns:
            DataFrame: Dataframe containing the loaded data.
        """
        self.logger.warning(
            "Reading data over HTTP/HTTPS. This may be slow for large datasets. Consider downloading the data to a distributed file system."
        )

        match fmt:
            case "csv":
                _header = kwargs.get("header", False)
                header = 0 if _header else None
                df = pd.read_csv(url, header=header, sep=kwargs.get("sep"))
            case "json":
                df = pd.read_json(url)
            case _:
                raise ValueError("Only csv, tsv and json are URL supported formats")
        if schema is None:
            return self.spark.createDataFrame(
                data=df,
                samplingRatio=kwargs.get("samplingRation", 0.4),
            )
        return self.spark.createDataFrame(data=df, schema=schema, verifySchema=True)

    def list_hadoop_paths(self, path: str) -> list[str]:
        """List files matching a Hadoop-compatible filesystem path or glob pattern.

        Delegates to Hadoop's ``FileSystem.globStatus``, which resolves the correct
        connector (GCS, S3A, HDFS, local…) from the URI scheme using auth already
        configured in the SparkContext. This is a driver-side metadata call — no data
        is read from the files themselves.

        Args:
            path (str): Hadoop path, optionally with glob wildcards
                (e.g. ``gs://bucket/prefix/*.tsv.gz``).

        Returns:
            list[str]: Sorted list of matching file path strings.
                Empty list when no files match the pattern.

        Examples:
            List all parquet files under a GCS prefix:

            >>> session.list_hadoop_paths("gs://bucket/data/*.parquet") # doctest: +SKIP
            ['gs://bucket/data/part-00000.parquet', 'gs://bucket/data/part-00001.parquet']

            Returns an empty list when nothing matches:

            >>> session.list_hadoop_paths("gs://bucket/nonexistent/*.tsv.gz") # doctest: +SKIP
            []
        """
        sc = self.spark.sparkContext
        # NOTE: The code below uses the PySpark JVM hadoop FileSystem to list objects
        # This is not the most elegant way of listing, but does not bound us to any
        # specific library to list objects through the object storage, rather
        # uses direct Hadoop FileSystem API which should work as long as the connector is properly configured in the session.
        jpath = sc._jvm.org.apache.hadoop.fs.Path(path)
        conf = sc._jsc.hadoopConfiguration()
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(jpath.toUri(), conf)
        statuses = fs.globStatus(jpath)
        return [] if statuses is None else sorted(str(s.getPath()) for s in statuses)


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

    def __init__(self, spark: SparkSession, level: str | None = None) -> None:
        """Log4j logger class. This class provides a wrapper around the Log4j logging system.

        Args:
            spark (SparkSession): The Spark session used to access Spark context and Log4j logging.
            level (str | None): Logging level. Defaults to provided by spark
        """
        log4j: Any = spark.sparkContext._jvm.org.apache.log4j  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess]
        if level:
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
