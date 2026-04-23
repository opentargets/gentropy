"""Hugging Face Hub credentials model."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, SecretStr, StringConstraints


class MissingHFTokenError(Exception):
    """Custom exception raised when the HF_TOKEN environment variable is not set."""
    pass

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
        >>> creds.token.get_secret_value()
        'hf_abc123'
        >>> os.unlink(path)
    """

    token: SecretStr

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


    @classmethod
    def read(cls, path: str | None = None) -> HuggingFaceHubCredentials:
        """Read Hugging Face Hub credentials from a JSON file or environment variable.

        If *path* is provided, the credentials will be loaded from the specified JSON file.
        If *path* is None, the method will attempt to read the token from the HF_TOKEN environment variable.

        Args:
            path (str | None): Optional path to the JSON credentials file. If None, the method will look for the HF_TOKEN environment variable.

        Returns:
            HuggingFaceHubCredentials: Validated credentials object.

        Raises:
            MissingHFTokenError: If *path* is None and the HF_TOKEN environment variable is not set.
            FileNotFoundError: If *path* is provided but the file does not exist.
            ValidationError: If the JSON file does not match the expected schema.

        """
        if path is None:
            import os
            token = os.getenv("HF_TOKEN")
            if not token:
                raise MissingHFTokenError("HF_TOKEN environment variable is not set.")
            return cls(token=SecretStr(token))

        return cls.from_json(path)


class HuggingFaceModelRepoHandle(BaseModel):
    """Information about a Hugging Face model repository.

    Attributes:
        repo_id (str): The identifier of the Hugging Face model repository, typically in the format "username/model_name".


    Examples:
        Create a HuggingFaceModelRepoHandle instance:

        >>> repo = HuggingFaceModelRepoHandle(handle="opentargets/locus2gene")
        >>> repo.handle
        'opentargets/locus2gene'
        >>> repo.repo_url()
        'https://huggingface.co/opentargets/locus2gene'
        >>> repo.repo_id()
        'locus2gene'
        >>> repo.username()
        'opentargets'
    """

    handle: Annotated[str, StringConstraints(pattern=r"^[\w-]+/[\w-]+$")]


    def repo_url(self) -> str:
        """Construct the URL for the Hugging Face model repository.

        Returns:
            str: The URL of the Hugging Face model repository.
        """
        return f"https://huggingface.co/{self.handle}"

    def repo_id(self) -> str:
        """Get the repository identifier.

        Returns:
            str: The identifier of the Hugging Face model repository.
        """
        return self.handle.split("/")[-1]

    def username(self) -> str:
        """Get the username associated with the repository.

        Returns:
            str: The username of the Hugging Face model repository.
        """
        return self.handle.split("/")[0]
