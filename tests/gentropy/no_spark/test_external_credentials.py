"""Tests for external vendor credential models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from gentropy.external.hf_hub import HuggingFaceHubCredentials, MissingHFTokenError
from gentropy.external.wandb import MissingWandbApiKeyError, WandbCredentials


class TestWandbCredentials:
    """Tests for WandbCredentials model."""

    WANDB_API_KEY = "test_wandb_api_key_1234567890abcdef"
    ENV_WANDB_API_KEY = "env_wandb_api_key_abcdef1234567890"
    WANDB_CREDENTIALS_PATH = (
        Path(__file__).parent.parent / "data_samples" / "wandb_credentials.json"
    )

    def test_from_json_loads_api_key(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = WandbCredentials.from_json(str(self.WANDB_CREDENTIALS_PATH))
        assert isinstance(creds.WANDB_API_KEY, SecretStr), "should be SecretStr"
        assert creds.WANDB_API_KEY.get_secret_value() == self.WANDB_API_KEY, (
            "API key different"
        )

    def test_from_json_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the credentials file does not exist."""
        with pytest.raises(FileNotFoundError):
            WandbCredentials.from_json(str(tmp_path / "missing.json"))

    def test_from_json_missing_required_field(self, tmp_path: Path) -> None:
        """ValidationError is raised when the JSON is missing the WANDB_API_KEY field."""
        bad_file = tmp_path / "bad_wandb.json"
        bad_file.write_text(json.dumps({"not_a_key": "value"}))
        with pytest.raises(ValidationError):
            WandbCredentials.from_json(str(bad_file))

    def test_from_json_invalid_json(self, tmp_path: Path) -> None:
        """ValueError is raised when the file contains malformed JSON."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {")
        with pytest.raises(ValidationError):
            WandbCredentials.from_json(str(bad_file))

    def test_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """read() loads credentials from a file when path is provided, and from environment variable when path is None."""
        # Test loading from file
        creds_from_file = WandbCredentials.read(self.WANDB_CREDENTIALS_PATH.as_posix())
        assert isinstance(creds_from_file.WANDB_API_KEY, SecretStr), (
            "should be SecretStr"
        )
        assert creds_from_file.WANDB_API_KEY.get_secret_value() == self.WANDB_API_KEY, (
            "API key different"
        )

        # Test loading from environment variable
        monkeypatch.setenv("WANDB_API_KEY", self.ENV_WANDB_API_KEY)
        creds_from_env = WandbCredentials.read()
        assert isinstance(creds_from_env.WANDB_API_KEY, SecretStr), (
            "should be SecretStr"
        )
        assert (
            creds_from_env.WANDB_API_KEY.get_secret_value() == self.ENV_WANDB_API_KEY
        ), "API key different"

        # Test error raised when env var is not set and no path provided
        monkeypatch.delenv("WANDB_API_KEY", raising=False)
        with pytest.raises(MissingWandbApiKeyError):
            WandbCredentials.read()


class TestHuggingFaceHubCredentials:
    """Tests for HuggingFaceHubCredentials model."""

    HF_TOKEN = "hf_test_token_1234567890abcdef"
    ENV_HF_TOKEN = "hf_env_token_abcdef1234567890"
    HF_HUB_CREDENTIALS_PATH = (
        Path(__file__).parent.parent / "data_samples" / "hf_hub_credentials.json"
    )

    def test_from_json_loads_token(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = HuggingFaceHubCredentials.from_json(str(self.HF_HUB_CREDENTIALS_PATH))
        assert isinstance(creds.HF_TOKEN, SecretStr), "should be SecretStr"
        assert creds.HF_TOKEN.get_secret_value() == self.HF_TOKEN, "HF_TOKEN different"

    def test_from_json_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the credentials file does not exist."""
        with pytest.raises(FileNotFoundError):
            HuggingFaceHubCredentials.from_json(str(tmp_path / "missing.json"))

    def test_from_json_missing_required_field(self, tmp_path: Path) -> None:
        """ValidationError is raised when the JSON is missing the token field."""
        bad_file = tmp_path / "bad_hf.json"
        bad_file.write_text(json.dumps({"not_a_token": "value"}))
        with pytest.raises(ValidationError):
            HuggingFaceHubCredentials.from_json(str(bad_file))

    def test_from_json_invalid_json(self, tmp_path: Path) -> None:
        """ValueError is raised when the file contains malformed JSON."""
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not valid json {")
        with pytest.raises(ValidationError):
            HuggingFaceHubCredentials.from_json(str(bad_file))

    def test_read(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """read() loads credentials from a file when path is provided, and from environment variable when path is None."""
        # Test loading from file
        creds_from_file = HuggingFaceHubCredentials.read(
            str(self.HF_HUB_CREDENTIALS_PATH)
        )
        assert isinstance(creds_from_file.HF_TOKEN, SecretStr), "should be SecretStr"
        assert creds_from_file.HF_TOKEN.get_secret_value() == self.HF_TOKEN, (
            "HF_TOKEN different"
        )

        # Test loading from environment variable
        monkeypatch.setenv("HF_TOKEN", self.ENV_HF_TOKEN)
        creds_from_env = HuggingFaceHubCredentials.read()
        assert isinstance(creds_from_env.HF_TOKEN, SecretStr), "should be SecretStr"
        assert creds_from_env.HF_TOKEN.get_secret_value() == self.ENV_HF_TOKEN, (
            "HF_TOKEN different"
        )

        # Test error raised when env var is not set and no path provided
        monkeypatch.delenv("HF_TOKEN", raising=False)
        with pytest.raises(MissingHFTokenError):
            HuggingFaceHubCredentials.read()
