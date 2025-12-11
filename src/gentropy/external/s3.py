"""Module for S3 configuration."""

from __future__ import annotations

import json

from pydantic import BaseModel


class S3Config(BaseModel):
    """Model for S3 configuration.

    Note:
        This configuration is used to connect to S3 compatible storage.
        Ensure that the access key and secret key have the necessary permissions
        to access the specified bucket. This configuration can be loaded from a JSON file
        using the `from_file` class method.

    Examples:
    ---
    >>> config = S3Config(
    ...     bucket_name="my-bucket",
    ...     s3_host_port=9000,
    ...     s3_host_url="s3.my-domain.com",
    ...     access_key_id="my-access-key-id",
    ...     secret_access_key="my-secret-access-key",
    ... )
    >>> print(config.bucket_name)
    my-bucket
    >>> print(config.s3_host_port)
    9000
    >>> print(config.s3_host_url)
    s3.my-domain.com
    >>> print(config.access_key_id)
    my-access-key-id
    >>> print(config.secret_access_key)
    my-secret-access-key
    """

    bucket_name: str
    """Name of the bucket, without s3:// or s3a:// prefix."""
    s3_host_port: int
    """Port number of the S3 host."""
    s3_host_url: str
    """URL of the S3 host."""
    access_key_id: str
    """Access key ID for S3 authentication."""
    secret_access_key: str
    """Secret access key for S3 authentication."""

    @classmethod
    def from_file(cls, path: str) -> S3Config:
        """Load S3 configuration from a file.

        Args:
            path (str): Path to the configuration file.

        Returns:
            S3Config: S3 configuration instance.

        Examples:
        ---
        >>> import tempfile
        >>> import os
        >>> config_data = {
        ...     "bucket_name": "my-bucket",
        ...     "s3_host_port": 9000,
        ...     "s3_host_url": "s3.my-domain.com",
        ...     "access_key_id": "my-access-key-id",
        ...     "secret_access_key": "my-secret-access-key",
        ... }
        >>> with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmpfile:
        ...     json.dump(config_data, tmpfile)
        ...     tmpfile_path = tmpfile.name
        >>> config = S3Config.from_file(tmpfile_path)
        >>> config.bucket_name
        'my-bucket'
        >>> os.remove(tmpfile_path)

        """
        with open(path, encoding="utf-8") as f:
            config_data = json.load(f)
        return cls(**config_data)
