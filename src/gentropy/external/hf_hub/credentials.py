"""Hugging Face Hub credentials model."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel


class HuggingFaceHubCredentials(BaseModel):
    """Credentials for Hugging Face Hub authentication.

    Attributes:
        token (str): HF Hub access token used to authenticate with the Hub API.

    Examples:
        Load credentials from a JSON file:

        >>> import json, tempfile, os
        >>> with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        ...     _ = f.write('{"token": "hf_abc123"}')
        ...     path = f.name
        >>> creds = HuggingFaceHubCredentials.from_json(path)
        >>> creds.token
        'hf_abc123'
        >>> os.unlink(path)
    """

    token: str

    @classmethod
    def from_json(cls, path: str) -> HuggingFaceHubCredentials:
        """Load Hugging Face Hub credentials from a JSON file.

        Args:
            path (str): Path to the JSON credentials file. The file must contain
                a ``token`` field.

        Returns:
            HuggingFaceHubCredentials: Validated credentials object.

        Raises:
            FileNotFoundError: If the file at *path* does not exist.
            ValidationError: If the JSON does not match the expected schema.
        """
        return cls.model_validate_json(Path(path).read_text())
