"""Tests that verify SparkSession configuration for S3 and GCS connectors.

These tests mock SparkSession.Builder.getOrCreate so the JVM session is never
started and no JARs are downloaded from the internet.  They exercise
_build_config / _setup_s3_connector / _setup_gcs_connector in isolation.
"""

from __future__ import annotations

from collections.abc import Generator
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from gentropy.common.session import Session
from gentropy.external.gcs import GCSAuthType, GCSConfig
from gentropy.external.s3 import S3Config

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_S3_DICT_CONFIG: dict[str, str | int] = {
    "bucket_name": "test-bucket",
    "access_key_id": "test-access-key",
    "secret_access_key": "test-secret-key",
    "s3_host_url": "s3.test-host.com",
    "s3_host_port": 9000,
}

_S3_CONFIG_FILE = "tests/gentropy/data_samples/example_s3_config.json"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _stop_active_spark() -> None:
    """Stop any active Spark session and clear cached references."""
    spark = SparkSession.getActiveSession()
    if spark is not None:
        spark.stop()


@pytest.fixture(scope="function")
def _no_spark_session() -> Generator[None, None, None]:
    """Ensure no active Spark session exists around the test."""
    _stop_active_spark()
    yield
    _stop_active_spark()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_session_no_spark(**session_kwargs: object) -> Session:
    """Return a Session whose SparkSession was never actually started.

    SparkSession.Builder.getOrCreate is replaced with a MagicMock so the JVM
    session (and JAR downloads) are bypassed entirely.
    """
    mock_spark = MagicMock(spec=SparkSession)
    with mock.patch.object(
        SparkSession.Builder, "getOrCreate", return_value=mock_spark
    ):
        return Session(**session_kwargs)  # type: ignore[arg-type]


def _conf_dict(
    session: Session,
    *,
    add_s3: bool = False,
    add_gcs: bool = False,
) -> dict[str, str]:
    """Re-invoke _build_config and return the resulting SparkConf as a plain dict.

    Calling _build_config a second time is safe: the connector setup methods
    write their parsed credentials into self._s3_configuration /
    self._gcs_configuration during __init__, so subsequent calls use those
    cached dicts rather than re-reading files or env vars.
    """
    conf = session._build_config(
        dynamic_allocation=False,
        start_hail=False,
        use_enhanced_bgzip_codec=False,
        add_s3_connector=add_s3,
        add_gcs_connector=add_gcs,
    )
    return dict(conf.getAll())


# ---------------------------------------------------------------------------
# S3 connector tests
# ---------------------------------------------------------------------------


@pytest.mark.no_shared_spark
class TestS3ConnectorConfiguration:
    """_build_config sets the correct S3A Hadoop keys when add_s3_connector=True."""

    @pytest.mark.usefixtures("_no_spark_session")
    def test_s3_connector_from_dict(self) -> None:
        """All mandatory S3A config keys are present when credentials are passed as a dict."""
        session = _create_session_no_spark(
            add_s3_connector=True,
            s3_configuration=_S3_DICT_CONFIG,
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_s3=True)

        assert (
            conf.get("spark.hadoop.fs.s3a.impl")
            == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        assert conf.get("spark.hadoop.fs.s3a.path.style.access") == "true"
        assert conf.get("spark.hadoop.fs.s3a.connection.ssl.enabled") == "true"
        assert (
            conf.get("spark.hadoop.fs.s3a.endpoint") == "https://s3.test-host.com:9000"
        )
        assert conf.get("spark.hadoop.fs.s3a.access.key") == "test-access-key"
        assert conf.get("spark.hadoop.fs.s3a.secret.key") == "test-secret-key"
        assert S3Config._HADOOP_CONNECTOR_PKG in conf.get("spark.jars.packages", "")

    @pytest.mark.usefixtures("_no_spark_session")
    def test_s3_connector_from_file(self) -> None:
        """All mandatory S3A config keys are present when credentials are loaded from a JSON file."""
        session = _create_session_no_spark(
            add_s3_connector=True,
            s3_configuration_path=_S3_CONFIG_FILE,
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_s3=True)

        # Values driven by tests/gentropy/data_samples/example_s3_config.json
        assert (
            conf.get("spark.hadoop.fs.s3a.impl")
            == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        assert conf.get("spark.hadoop.fs.s3a.access.key") == "my_access_key"
        assert conf.get("spark.hadoop.fs.s3a.secret.key") == "my_secret_access"
        assert S3Config._HADOOP_CONNECTOR_PKG in conf.get("spark.jars.packages", "")

    @pytest.mark.usefixtures("_no_spark_session")
    def test_s3_connector_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """All mandatory S3A config keys are present when credentials come from environment variables."""
        monkeypatch.setenv("AWS_S3_BUCKET_NAME", "env-bucket")
        monkeypatch.setenv("AWS_ENDPOINT_URL", "env-s3-host.com:4430")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-access-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret-key")

        session = _create_session_no_spark(
            add_s3_connector=True,
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_s3=True)

        assert (
            conf.get("spark.hadoop.fs.s3a.impl")
            == "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        assert (
            conf.get("spark.hadoop.fs.s3a.endpoint") == "https://env-s3-host.com:4430"
        )
        assert conf.get("spark.hadoop.fs.s3a.access.key") == "env-access-key"
        assert conf.get("spark.hadoop.fs.s3a.secret.key") == "env-secret-key"
        assert S3Config._HADOOP_CONNECTOR_PKG in conf.get("spark.jars.packages", "")

    @pytest.mark.usefixtures("_no_spark_session")
    def test_s3_connector_anonymous(self) -> None:
        """Anonymous mode configures Hadoop without requiring credentials."""
        session = _create_session_no_spark(
            add_s3_connector=True,
            s3_configuration={"anonymous": True},
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_s3=True)

        assert (
            conf.get("spark.hadoop.fs.s3a.aws.credentials.provider")
            == "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        )
        assert "spark.hadoop.fs.s3a.access.key" not in conf
        assert "spark.hadoop.fs.s3a.secret.key" not in conf


# ---------------------------------------------------------------------------
# GCS connector tests
# ---------------------------------------------------------------------------


@pytest.mark.no_shared_spark
class TestGCSConnectorConfiguration:
    """_build_config sets the correct GCS Hadoop connector keys when add_gcs_connector=True."""

    @pytest.mark.usefixtures("_no_spark_session")
    def test_gcs_compute_engine_connector(self) -> None:
        """Core GCS config keys are present with COMPUTE_ENGINE auth (the Dataproc default)."""
        session = _create_session_no_spark(
            add_gcs_connector=True,
            gcs_configuration={"project_id": "my-gcp-project"},
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_gcs=True)

        assert (
            conf.get("spark.hadoop.fs.gs.impl")
            == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        assert (
            conf.get("spark.hadoop.fs.AbstractFileSystem.gs.impl")
            == "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        assert conf.get("spark.hadoop.fs.gs.auth.type") == GCSAuthType.COMPUTE_ENGINE
        assert conf.get("spark.hadoop.fs.gs.project.id") == "my-gcp-project"
        assert conf.get("spark.hadoop.fs.gs.status.parallel.enable") == "true"
        assert conf.get("spark.hadoop.fs.gs.copy.with.rewrite.enable") == "true"
        assert conf.get("spark.hadoop.fs.gs.glob.algorithm") == "CONCURRENT"
        assert GCSConfig._HADOOP_CONNECTOR_JAR in conf.get("spark.jars", "")

    @pytest.mark.usefixtures("_no_spark_session")
    def test_gcs_service_account_keyfile_connector(self) -> None:
        """Optional GCS config keys are set when SERVICE_ACCOUNT_JSON_KEYFILE auth is used.

        This exercises the conditional branches in _setup_gcs_connector that only
        fire when keyfile_path, impersonation_sa, and requester-pays fields are set.
        """
        session = _create_session_no_spark(
            add_gcs_connector=True,
            gcs_configuration={
                "auth_type": GCSAuthType.SERVICE_ACCOUNT_JSON_KEYFILE,
                "keyfile_path": "/path/to/service-account.json",
                "impersonation_sa": "target-sa@other-project.iam.gserviceaccount.com",
                "requester_pays": "ENABLED",
                "requester_pays_project_id": "billing-project",
            },
            dynamic_allocation=False,
        )
        conf = _conf_dict(session, add_gcs=True)

        assert (
            conf.get("spark.hadoop.fs.gs.auth.type")
            == GCSAuthType.SERVICE_ACCOUNT_JSON_KEYFILE
        )
        assert (
            conf.get("spark.hadoop.fs.gs.auth.service.account.json.keyfile")
            == "/path/to/service-account.json"
        )
        assert (
            conf.get("spark.hadoop.fs.gs.auth.impersonation.service.account")
            == "target-sa@other-project.iam.gserviceaccount.com"
        )
        assert conf.get("spark.hadoop.fs.gs.requester.pays.mode") == "ENABLED"
        assert (
            conf.get("spark.hadoop.fs.gs.requester.pays.project.id")
            == "billing-project"
        )
        assert GCSConfig._HADOOP_CONNECTOR_JAR in conf.get("spark.jars", "")
