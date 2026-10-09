"""Shared base classes for external service configuration and credentials."""

from __future__ import annotations

import os
from pathlib import Path
from typing import ClassVar, Self

from pydantic import BaseModel, SecretStr


class ExternalConfig(BaseModel):
    """Base class for all external service configuration models.

    Provides a shared :meth:`from_json` implementation so that every subclass
    can deserialise itself from a JSON file without repeating the same one-liner.
    """

    @classmethod
    def from_json(cls, path: str) -> Self:
        """Load configuration from a JSON file.

        Args:
            path (str): Path to the JSON file.

        Returns:
            Self: Validated configuration object.

        Raises:
            FileNotFoundError: If the file at *path* does not exist.
            ValidationError: If the JSON does not match the expected schema.
        """
        return cls.model_validate_json(Path(path).read_text())


class MissingApiKeyError(Exception):
    """Raised when a required API key environment variable is not set."""

    pass


class BaseServiceCredentials(ExternalConfig):
    """Base class for single-secret external service credentials.

    Subclasses must declare a ``_env_var`` class variable whose value is both
    the name of the environment variable and the Pydantic field that holds the
    secret.

    Examples:
        >>> class MyServiceCredentials(BaseServiceCredentials):
        ...     _env_var: ClassVar[str] = "MY_API_KEY"
        ...     MY_API_KEY: SecretStr
    """

    _env_var: ClassVar[str]

    @classmethod
    def read(cls, path: str | None = None) -> Self:
        """Read credentials from a JSON file or environment variable.

        Args:
            path (str | None): Path to a JSON credentials file. If ``None``,
                the credential is read from the environment variable named by
                ``_env_var``.

        Raises:
            MissingApiKeyError: If *path* is ``None`` and the environment
                variable is not set.
            FileNotFoundError: If *path* is provided but the file does not exist.
            ValidationError: If the JSON file does not match the expected schema.
        """
        if path is not None:
            return cls.from_json(path)
        key = os.getenv(cls._env_var)
        if not key:
            raise MissingApiKeyError(f"{cls._env_var} environment variable is not set.")
        return cls(**{cls._env_var: SecretStr(key)})
