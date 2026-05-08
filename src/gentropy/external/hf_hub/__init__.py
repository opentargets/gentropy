"""Hugging Face Hub credentials model."""

from __future__ import annotations

from typing import Annotated, ClassVar

from pydantic import BaseModel, SecretStr, StringConstraints

from gentropy.external import BaseServiceCredentials


class HuggingFaceHubCredentials(BaseServiceCredentials):
    """Credentials for Hugging Face Hub authentication.

    Attributes:
        HF_TOKEN (SecretStr): HF Hub access token used to authenticate with the Hub API.

    Examples:
        Load credentials from a JSON file:

        >>> import json, tempfile, os
        >>> with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        ...     _ = f.write('{"HF_TOKEN": "hf_abc123"}')
        ...     path = f.name
        >>> creds = HuggingFaceHubCredentials.from_json(path)
        >>> creds.HF_TOKEN.get_secret_value()
        'hf_abc123'
        >>> os.unlink(path)
    """

    _env_var: ClassVar[str] = "HF_TOKEN"
    HF_TOKEN: SecretStr


class HuggingFaceModelRepoHandle(BaseModel):
    """Information about a Hugging Face model repository.

    Attributes:
        repo_id (str): The identifier of the Hugging Face model repository, typically in the format "username/model_name".

    Note:
        Regex pattern used for validation is derived from :class:`huggingface_hub.utils._validators.REPO_ID_REGEX` — requires namespace/repo_name,
        alphanumeric + . _ - allowed, cannot start or end with . or -, repo name capped at 96 chars.

    Examples:
        Create a HuggingFaceModelRepoHandle instance:

        >>> repo = HuggingFaceModelRepoHandle(handle="opentargets/locus_to_gene_26.06.0-dev0")
        >>> repo.handle
        'opentargets/locus_to_gene_26.06.0-dev0'
        >>> repo.repo_url()
        'https://huggingface.co/opentargets/locus_to_gene_26.06.0-dev0'
        >>> repo.repo_id()
        'locus_to_gene_26.06.0-dev0'
        >>> repo.username()
        'opentargets'
    """

    handle: Annotated[str, StringConstraints(pattern=r"^\b[\w.-]+\b/\b[\w.-]{1,96}\b$")]

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
