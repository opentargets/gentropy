"""Weights & Biases credentials model."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, SecretStr


class MissingWandbApiKeyError(Exception):
    """Custom exception raised when the WANDB_API_KEY environment variable is not set."""
    pass

class WandbCredentials(BaseModel):
    """Credentials for Weights & Biases authentication.

    Attributes:
        api_key (SecretStr): W&B API key used to authenticate with the W&B service.

    Examples:
        Load credentials from a JSON file:

        >>> import json, tempfile, os
        >>> with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        ...     _ = f.write('{"api_key": "my_key"}')
        ...     path = f.name
        >>> creds = WandbCredentials.from_json(path)
        >>> creds.api_key.get_secret_value()
        'my_key'
        >>> os.unlink(path)
    """

    api_key: SecretStr

    @classmethod
    def from_json(cls, path: str) -> WandbCredentials:
        """Load W&B credentials from a JSON file.

        Args:
            path (str): Path to the JSON credentials file. The file must contain
                an ``api_key`` field.

        Returns:
            WandbCredentials: Validated credentials object.

        Raises:
            FileNotFoundError: If the file at *path* does not exist.
            ValidationError: If the JSON does not match the expected schema.
        """
        return cls.model_validate_json(Path(path).read_text())


    @classmethod
    def read(cls, path: str | None = None) -> WandbCredentials:
        """Read W&B credentials from a JSON file or environment variable.

        If *path* is provided, the credentials will be loaded from the specified JSON file.
        If *path* is None, the method will attempt to read the API key from the WANDB_API_KEY environment variable.

        Args:
            path (str | None): Optional path to the JSON credentials file. If None, the method will look for the WANDB_API_KEY environment variable.
        """
        if path is not None:
            return cls.from_json(path)
        else:
            import os
            api_key = os.getenv("WANDB_API_KEY")
            if api_key is None:
                raise MissingWandbApiKeyError("WANDB_API_KEY environment variable is not set.")
            return cls(api_key=SecretStr(api_key))
