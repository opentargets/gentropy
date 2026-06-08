"""Common functions for Google Cloud Storage (GCS) operations."""

from __future__ import annotations

import os
from enum import StrEnum
from typing import ClassVar

from pydantic import model_validator

from gentropy.external import ExternalConfig


class GCSAuthType(StrEnum):
    """Authentication mechanism for the GCS Hadoop connector (fs.gs.auth.type)."""

    APPLICATION_DEFAULT = "APPLICATION_DEFAULT"
    COMPUTE_ENGINE = "COMPUTE_ENGINE"
    SERVICE_ACCOUNT_JSON_KEYFILE = "SERVICE_ACCOUNT_JSON_KEYFILE"
    UNAUTHENTICATED = "UNAUTHENTICATED"


class GCSRequesterPaysMode(StrEnum):
    """Requester-pays billing mode for GCS bucket access (fs.gs.requester.pays.mode)."""

    DISABLED = "DISABLED"
    AUTO = "AUTO"
    CUSTOM = "CUSTOM"
    ENABLED = "ENABLED"


class GCSConfig(ExternalConfig):
    """Model for Google Cloud Storage connector configuration.

    Note:
        This configuration is used to set optional GCS-connector parameters when
        building a Spark session via :class:`~gentropy.common.session.Session`.

        ``project_id`` is only required for bucket-level operations (list/create)
        and as a billing project fallback when requester pays is active.

        The configuration can be loaded from a JSON file using :meth:`from_json`,
        from environment variables using :meth:`from_env`, or by calling
        :meth:`read` which tries both in order.

    Examples:
    ---
    Default configuration — uses the Dataproc VM's attached service account, no requester pays:

    >>> config = GCSConfig()
    >>> config.auth_type
    <GCSAuthType.COMPUTE_ENGINE: 'COMPUTE_ENGINE'>
    >>> config.requester_pays
    <GCSRequesterPaysMode.DISABLED: 'DISABLED'>

    Cross-project access with service account impersonation:

    >>> config = GCSConfig(
    ...     auth_type=GCSAuthType.COMPUTE_ENGINE,
    ...     impersonation_sa="target-sa@other-project.iam.gserviceaccount.com",
    ...     requester_pays=GCSRequesterPaysMode.ENABLED,
    ...     requester_pays_project_id="billing-project",
    ... )
    >>> config.impersonation_sa
    'target-sa@other-project.iam.gserviceaccount.com'

    Explicit JSON keyfile (e.g. local dev without metadata server):

    >>> config = GCSConfig(
    ...     auth_type=GCSAuthType.SERVICE_ACCOUNT_JSON_KEYFILE,
    ...     keyfile_path="/path/to/key.json",
    ... )
    >>> config.keyfile_path
    '/path/to/key.json'

    Requester-pays for specific buckets only:

    >>> config = GCSConfig(
    ...     requester_pays=GCSRequesterPaysMode.CUSTOM,
    ...     requester_pays_buckets=["paid-bucket-1", "paid-bucket-2"],
    ...     requester_pays_project_id="billing-project",
    ... )
    >>> config.requester_pays_buckets
    ['paid-bucket-1', 'paid-bucket-2']
    """

    _HADOOP_CONNECTOR_PKG: ClassVar[str] = (
        "com.google.cloud.bigdataoss:gcs-connector:4.0.4"
    )
    """Connector for Google Cloud Storage.
        See https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector/4.0.4"""

    project_id: str | None = None
    """Google Cloud Project ID. Required only for list-buckets and create-bucket operations,
    and used as the billing project fallback when requester pays is active."""

    auth_type: GCSAuthType = GCSAuthType.COMPUTE_ENGINE
    """Authentication mechanism. Defaults to COMPUTE_ENGINE (Dataproc VM attached service account)."""

    keyfile_path: str | None = None
    """Path to the JSON service account key file. Required when auth_type is SERVICE_ACCOUNT_JSON_KEYFILE.
    The file must exist at the same path on all cluster nodes."""

    impersonation_sa: str | None = None
    """Service account to impersonate for all requests. The base credential (auth_type) is used
    to mint short-lived tokens for this target service account."""

    requester_pays: GCSRequesterPaysMode = GCSRequesterPaysMode.DISABLED
    """Requester-pays billing mode. Defaults to DISABLED (caller's project is not billed)."""

    requester_pays_project_id: str | None = None
    """Billing project for requester-pays requests. Falls back to project_id if not set."""

    requester_pays_buckets: list[str] | None = None
    """Buckets subject to requester-pays billing. Required when requester_pays is CUSTOM."""

    @model_validator(mode="after")
    def _validate_conditional_fields(self) -> GCSConfig:
        if (
            self.auth_type == GCSAuthType.SERVICE_ACCOUNT_JSON_KEYFILE
            and not self.keyfile_path
        ):
            raise ValueError(
                "keyfile_path is required when auth_type is SERVICE_ACCOUNT_JSON_KEYFILE"
            )
        if (
            self.requester_pays == GCSRequesterPaysMode.CUSTOM
            and not self.requester_pays_buckets
        ):
            raise ValueError(
                "requester_pays_buckets is required when requester_pays mode is CUSTOM"
            )
        return self

    @classmethod
    def from_env(cls) -> GCSConfig:
        """Load GCS configuration from environment variables.

        Environment variables:
            GCS_PROJECT_ID: Google Cloud Project ID.
            GCS_AUTH_TYPE: Authentication type (default: COMPUTE_ENGINE).
            GCS_KEYFILE_PATH: Path to JSON keyfile (required when GCS_AUTH_TYPE=SERVICE_ACCOUNT_JSON_KEYFILE).
            GCS_IMPERSONATION_SA: Service account to impersonate.
            GCS_REQUESTER_PAYS: Requester-pays mode (default: DISABLED).
            GCS_REQUESTER_PAYS_PROJECT_ID: Billing project for requester-pays requests.
            GCS_REQUESTER_PAYS_BUCKETS: Comma-separated list of buckets subject to requester pays.

        Returns:
            GCSConfig: GCS configuration instance.

        Examples:
        ---
        >>> import os
        >>> os.environ["GCS_PROJECT_ID"] = "my-gcp-project"
        >>> os.environ["GCS_IMPERSONATION_SA"] = "sa@other-project.iam.gserviceaccount.com"
        >>> config = GCSConfig.from_env()
        >>> config.project_id
        'my-gcp-project'
        >>> config.impersonation_sa
        'sa@other-project.iam.gserviceaccount.com'
        """
        buckets_raw = os.getenv("GCS_REQUESTER_PAYS_BUCKETS")
        return cls(
            project_id=os.getenv("GCS_PROJECT_ID"),
            auth_type=os.getenv("GCS_AUTH_TYPE", GCSAuthType.COMPUTE_ENGINE),
            keyfile_path=os.getenv("GCS_KEYFILE_PATH"),
            impersonation_sa=os.getenv("GCS_IMPERSONATION_SA"),
            requester_pays=os.getenv(
                "GCS_REQUESTER_PAYS", GCSRequesterPaysMode.DISABLED
            ),
            requester_pays_project_id=os.getenv("GCS_REQUESTER_PAYS_PROJECT_ID"),
            requester_pays_buckets=buckets_raw.split(",") if buckets_raw else None,
        )

    @classmethod
    def read(cls, path: str | None = None) -> GCSConfig:
        """Read from a JSON file or environment variables.

        Args:
            path (str | None): Optional path to a JSON configuration file.
                If not provided, the method will attempt to load from
                environment variables.

        Returns:
            GCSConfig: GCS configuration instance.
        """
        if path is not None:
            return cls.from_json(path)
        return cls.from_env()


def copy_to_gcs(source_path: str, destination_blob: str) -> None:
    """Copy a file to a Google Cloud Storage bucket.

    Args:
        source_path (str): Path to the local file to copy
        destination_blob (str): GS path to the destination blob in the GCS bucket

    Raises:
        ValueError: If the path is a directory
    """
    import os
    from urllib.parse import urlparse

    from google.cloud import storage

    if os.path.isdir(source_path):
        raise ValueError("Path should be a file, not a directory.")
    client = storage.Client()
    bucket = client.bucket(bucket_name=urlparse(destination_blob).hostname)
    blob = bucket.blob(blob_name=urlparse(destination_blob).path.lstrip("/"))
    blob.upload_from_filename(source_path)
