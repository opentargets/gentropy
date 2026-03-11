"""Weights & Biases credentials model."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel


class WandbCredentials(BaseModel):
    """Credentials for Weights & Biases authentication.

    Attributes:
        api_key (str): W&B API key used to authenticate with the W&B service.

    Examples:
        Load credentials from a JSON file:

        >>> import json, tempfile, os
        >>> with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        ...     _ = f.write('{"api_key": "my_key"}')
        ...     path = f.name
        >>> creds = WandbCredentials.from_json(path)
        >>> creds.api_key
        'my_key'
        >>> os.unlink(path)
    """

    api_key: str

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
