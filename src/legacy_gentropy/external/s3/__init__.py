"""Module for S3 configuration."""

from __future__ import annotations

import os
from typing import ClassVar

from pydantic import SecretStr

from gentropy.external import ExternalConfig


class S3Config(ExternalConfig):
    """Model for S3 configuration.

    Note:
        This configuration is used to connect to S3 compatible storage.
        Ensure that the access key and secret key have the necessary permissions
        to access the specified bucket. This configuration can be loaded from a JSON file
        using the `from_json` class method.

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
    >>> print(config.access_key_id.get_secret_value())
    my-access-key-id
    >>> print(config.secret_access_key.get_secret_value())
    my-secret-access-key
    """

    _HADOOP_CONNECTOR_PKG: ClassVar[str] = (
        "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367"
    )
    """Connector for AWS S3 compatible storage.
        See https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.6"""

    bucket_name: str
    """Name of the bucket, without s3:// or s3a:// prefix."""
    s3_host_port: int
    """Port number of the S3 host."""
    s3_host_url: str
    """URL of the S3 host."""
    access_key_id: SecretStr
    """Access key ID for S3 authentication."""
    secret_access_key: SecretStr
    """Secret access key for S3 authentication."""

    @classmethod
    def from_env(cls) -> S3Config:
        """Load S3 configuration from environment variables.

        Returns:
            S3Config: S3 configuration instance.

        !!! note:
            The following environment variables must be set:
            - `AWS_S3_BUCKET_NAME`: Name of the S3 bucket.
            - `AWS_ENDPOINT_URL`: Endpoint URL of the S3 service in the format 'host:port'.
            - `AWS_ACCESS_KEY_ID`: Access key ID for S3 authentication.
            - `AWS_SECRET_ACCESS_KEY`: Secret access key for S3 authentication.

        Examples:
        ---
        >>> import os
        >>> os.environ["AWS_S3_BUCKET_NAME"] = "my-bucket"
        >>> os.environ["AWS_ENDPOINT_URL"] = "s3.my-domain.com:9000"
        >>> os.environ["AWS_ACCESS_KEY_ID"] = "my-access-key-id"
        >>> os.environ["AWS_SECRET_ACCESS_KEY"] = "my-secret-access-key"
        >>> config = S3Config.from_env()
        >>> config.bucket_name
        'my-bucket'
        """
        bucket = os.getenv("AWS_S3_BUCKET_NAME")
        if bucket is None:
            raise ValueError("AWS_S3_BUCKET_NAME environment variable is not set.")

        endpoint_url = os.getenv("AWS_ENDPOINT_URL", "s3.amazonaws.com")
        if not endpoint_url:
            raise ValueError("AWS_ENDPOINT_URL environment variable is not set.")

        _split = endpoint_url.split(":")
        if not len(_split) == 2:
            raise ValueError(
                "AWS_ENDPOINT_URL environment variable must be in the format 'host:port'."
            )
        s3_host_url = _split[0]
        s3_host_port_str = _split[1]
        if not s3_host_port_str.isdigit():
            raise ValueError(
                "Port number in AWS_ENDPOINT_URL environment variable must be an integer."
            )

        access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        if access_key_id is None:
            raise ValueError("AWS_ACCESS_KEY_ID environment variable is not set.")

        secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if secret_access_key is None:
            raise ValueError("AWS_SECRET_ACCESS_KEY environment variable is not set.")

        return cls(
            bucket_name=bucket,
            s3_host_port=int(s3_host_port_str),
            s3_host_url=s3_host_url,
            access_key_id=SecretStr(access_key_id),
            secret_access_key=SecretStr(secret_access_key),
        )

    @classmethod
    def read(cls, path: str | None = None) -> S3Config:
        """Read from file or environment variables.

        Args:
            path (str | None): Optional path to the configuration file. If not provided,
                the method will attempt to load from environment variables.

        Returns:
            S3Config: S3 configuration instance.
        """
        if path is not None:
            return cls.from_json(path)
        return cls.from_env()
