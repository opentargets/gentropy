"""Tests that need to create their own Spark session."""

from collections.abc import Generator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as t

from gentropy import Session
from gentropy.common.session import SparkWriteMode
from gentropy.datasource.finngen_meta import MetaAnalysisDataSource
from gentropy.datasource.finngen_meta.summary_statistics import (
    FinnGenUkbMvpMetaSummaryStatistics,
)
from utils.spark import get_spark_testing_conf


def _stop_active_spark() -> None:
    """Stop any active Spark session and clear cached references."""
    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.stop()


@pytest.fixture(scope="function")
def _no_spark_session() -> Generator[None, None, None]:
    """Clean up any active spark session."""
    _stop_active_spark()
    yield
    _stop_active_spark()


@pytest.mark.no_spark
class TestNoSpark:
    """Test functionalities that require the spark session stopped."""

    ex_conf = dict(get_spark_testing_conf().getAll())

    @pytest.mark.usefixtures("_no_spark_session")
    def test_session_creation(self) -> None:
        """Test session creation with mock data."""
        spark = Session(spark_uri="local[1]", extended_spark_conf=self.ex_conf)
        assert isinstance(spark, Session)

    @pytest.mark.usefixtures("_no_spark_session")
    def test_output_partition(self) -> None:
        """Test output partition setting."""
        session = Session(
            spark_uri="local[1]",
            output_partitions=5,
            write_mode=SparkWriteMode.OVERWRITE,
            extended_spark_conf=self.ex_conf,
        )
        assert session.output_partitions == 5
        assert session.write_mode == SparkWriteMode.OVERWRITE

    @pytest.mark.usefixtures("_no_spark_session")
    def test_bgzip_configuration(self) -> None:
        """Assert that Hail configuration is set when use_bgzip is True."""
        session = Session(
            spark_uri="local[1]",
            use_enhanced_bgzip_codec=True,
            extended_spark_conf=self.ex_conf,
        )

        expected_bgzip_conf = {
            "spark.jars.packages": "org.seqdoop:hadoop-bam:7.10.0",
            "spark.hadoop.io.compression.codecs": "org.seqdoop.hadoop_bam.util.BGZFEnhancedGzipCodec",
            "spark.gentropy.useEnhancedBgzipCodec": "true",
        }

        observed_conf = dict(session.spark.sparkContext.getConf().getAll())
        for key, value in expected_bgzip_conf.items():
            assert observed_conf.get(key) == value, (
                f"Expected {key} to be set to {value}"
            )

    @pytest.mark.usefixtures("_no_spark_session")
    def test_hail_configuration(self, hail_home: str) -> None:
        """Assert that Hail configuration is set when start_hail is True."""
        session = Session(
            spark_uri="local[1]",
            hail_home=hail_home,
            start_hail=True,
            extended_spark_conf=self.ex_conf,
        )

        expected_hail_conf = {
            "spark.jars": f"{hail_home}/backend/hail-all-spark.jar",
            "spark.driver.extraClassPath": f"{hail_home}/backend/hail-all-spark.jar",
            "spark.executor.extraClassPath": "./hail-all-spark.jar",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrator": "is.hail.kryo.HailKryoRegistrator",
        }

        observed_conf = dict(session.spark.sparkContext.getConf().getAll())
        for key, value in expected_hail_conf.items():
            assert observed_conf.get(key) == value, (
                f"Expected {key} to be set to {value}"
            )

    @pytest.mark.webtest
    @pytest.mark.usefixtures("_no_spark_session")
    def test_bgzip_from_parquet_with_codec(self, tmp_path: Path) -> None:
        """Test bgzip codec usage on multiple tsv.gz files with different schemas.

        Note:
            This test downloads hadoop-bam and its dependencies from Maven Central
            to a temporary ivy cache on first run. This test needs to be run in complete isolation
            with access to the internet to download the dependencies. Hence this test is marked with 'webtest' and should be run separately from other tests.
        """
        ivy_cache_dir = tmp_path / "ivy_cache"
        ivy_cache_dir.mkdir(parents=True, exist_ok=True)
        conf = self.ex_conf.copy()
        conf["spark.jars.ivy"] = ivy_cache_dir.as_posix()
        session = Session(
            spark_uri="local[1]",
            extended_spark_conf=conf,
            use_enhanced_bgzip_codec=True,
            dynamic_allocation=False,
        )

        # Create inputs with different schemas
        input_path_1 = "tests/gentropy/data_samples/bgzip_tests/A.tsv.gz"  # contains only chr, pos, ref, alt, snp
        input_path_2 = "tests/gentropy/data_samples/bgzip_tests/B.tsv.gz"  # contains only chr, pos, ref, alt, fg_beta,
        output_path = (tmp_path / "output").as_posix()

        # Assert that test files & tbi indices exist
        for p in [input_path_1, input_path_2]:
            assert Path(p).exists(), f"Test file {p} does not exist."
            assert Path(p + ".tbi").exists(), f"Index file {p}.tbi does not exist."
        FinnGenUkbMvpMetaSummaryStatistics.bgzip_to_parquet(
            session,
            summary_statistics_list=[input_path_1, input_path_2],
            datasource=MetaAnalysisDataSource.FINNGEN_UKBB,
            raw_summary_statistics_output_path=output_path,
        )
        # Now read back the parquet files and check if schema is equal to raw schema
        df = session.spark.read.parquet(output_path)
        expected_schema = FinnGenUkbMvpMetaSummaryStatistics.raw_schema
        expected_schema = expected_schema.add(
            "studyId", t.StringType(), nullable=True
        )  # studyId is added during bgzip_to_parquet
        assert df.schema == expected_schema, "Schemas do not match after conversion."
        session.spark.stop()
