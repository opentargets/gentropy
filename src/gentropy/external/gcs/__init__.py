"""Common functions for Google Cloud Storage (GCS) operations."""

from __future__ import annotations

import os
from typing import ClassVar, Literal

from gentropy.external import ExternalConfig


class GCSConfig(ExternalConfig):
    """Model for Google Cloud Storage connector configuration.

    Note:
        This configuration is used to set optional GCS-connector parameters when
        building a Spark session via :class:`~gentropy.common.session.Session`.
        ``project_id`` is only required for bucket-level operations (list/create);
        it is not needed for ordinary read/write access to objects.

        The configuration can be loaded from a JSON file using :meth:`from_json`,
        from the ``GCS_PROJECT_ID`` environment variable using :meth:`from_env`,
        or by calling :meth:`read` which tries both in order.

    Examples:
    ---
    >>> config = GCSConfig(project_id="my-gcp-project")
    >>> print(config.project_id)
    my-gcp-project

    >>> config_empty = GCSConfig()
    >>> config_empty.project_id is None
    True
    """

    _HADOOP_CONNECTOR_PKG: ClassVar[str] = (
        "com.google.cloud.bigdataoss:gcs-connector:4.0.4"
    )
    """Connector for Google Cloud Storage.
        See https://mvnrepository.com/artifact/com.google.cloud.bigdataoss/gcs-connector/4.0.4"""

    project_id: str
    """Google Cloud Project ID. Required only for list-buckets and create-bucket operations."""
    requester_pays: Literal["ENABLED"] = "ENABLED"
    """Whether to enable Requester Pays for all GCS buckets (sets fs.gs.requester.pays.mode=ENABLED)."""

    @classmethod
    def from_env(cls) -> GCSConfig:
        """Load GCS configuration from environment variables.

        Reads ``GCS_PROJECT_ID`` if set; otherwise returns a config with
        ``project_id=None`` (sufficient for most read/write workloads).

        Returns:
            GCSConfig: GCS configuration instance.

        Examples:
        ---
        >>> import os
        >>> os.environ["GCS_PROJECT_ID"] = "my-gcp-project"
        >>> os.environ["GCS_REQUESTER_PAYS"] = "ENABLED"
        >>> config = GCSConfig.from_env()
        >>> config.project_id
        'my-gcp-project'
        >>> config.requester_pays
        'ENABLED'
        """
        return cls(
            project_id=os.getenv("GCS_PROJECT_ID"),
            requester_pays=os.getenv("GCS_REQUESTER_PAYS"),
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
