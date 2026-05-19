"""Weights & Biases credentials model."""

from __future__ import annotations

from typing import ClassVar

from pydantic import SecretStr

from gentropy.external import BaseServiceCredentials


class WandbCredentials(BaseServiceCredentials):
    """Credentials for Weights & Biases authentication.

    Attributes:
        WANDB_API_KEY (SecretStr): W&B API key used to authenticate with the W&B service.

    Examples:
        Load credentials from a JSON file:

        >>> import json, tempfile, os
        >>> with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        ...     _ = f.write('{"WANDB_API_KEY": "my_key"}')
        ...     path = f.name
        >>> creds = WandbCredentials.from_json(path)
        >>> creds.WANDB_API_KEY.get_secret_value()
        'my_key'
        >>> os.unlink(path)
    """

    _env_var: ClassVar[str] = "WANDB_API_KEY"
    WANDB_API_KEY: SecretStr
