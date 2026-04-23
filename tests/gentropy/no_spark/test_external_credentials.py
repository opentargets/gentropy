"""Tests for external vendor credential models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from gentropy.external.hf_hub import HuggingFaceHubCredentials, MissingHFTokenError
from gentropy.external.wandb import MissingWandbApiKeyError, WandbCredentials

DATA_SAMPLES = Path(__file__).parent.parent / "data_samples"
WANDB_CREDENTIALS_PATH = DATA_SAMPLES / "wandb_credentials.json"
HF_HUB_CREDENTIALS_PATH = DATA_SAMPLES / "hf_hub_credentials.json"


class TestWandbCredentials:
    """Tests for WandbCredentials model."""

    def test_from_json_loads_api_key(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = WandbCredentials.from_json(str(WANDB_CREDENTIALS_PATH))
        assert isinstance(creds.api_key, SecretStr), (
            "api_key should be a SecretStr instance"
        )
        assert creds.api_key.get_secret_value() == "test_wandb_api_key_1234567890abcdef"

    def test_from_json_file_not_found(self, tmp_path: Path) -> None:
        """FileNotFoundError is raised when the credentials file does not exist."""
        with pytest.raises(FileNotFoundError):
            WandbCredentials.from_json(str(tmp_path / "missing.json"))

    def test_from_json_missing_required_field(self, tmp_path: Path) -> None:
        """ValidationError is raised when the JSON is missing the api_key field."""
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
        creds_from_file = WandbCredentials.read(str(WANDB_CREDENTIALS_PATH))
        assert isinstance(creds_from_file.api_key, SecretStr), (
            "api_key should be a SecretStr instance"
        )
        assert (
            creds_from_file.api_key.get_secret_value()
            == "test_wandb_api_key_1234567890abcdef"
        )

        # Test loading from environment variable
        monkeypatch.setenv("WANDB_API_KEY", "env_wandb_api_key_abcdef1234567890")
        creds_from_env = WandbCredentials.read()
        assert isinstance(creds_from_env.api_key, SecretStr), (
            "api_key should be a SecretStr instance"
        )
        assert (
            creds_from_env.api_key.get_secret_value()
            == "env_wandb_api_key_abcdef1234567890"
        )

        # Test error raised when env var is not set and no path provided
        monkeypatch.delenv("WANDB_API_KEY", raising=False)
        with pytest.raises(MissingWandbApiKeyError):
            WandbCredentials.read()


class TestHuggingFaceHubCredentials:
    """Tests for HuggingFaceHubCredentials model."""

    def test_from_json_loads_token(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = HuggingFaceHubCredentials.from_json(str(HF_HUB_CREDENTIALS_PATH))
        assert isinstance(creds.token, SecretStr), (
            "token should be a SecretStr instance"
        )
        assert creds.token.get_secret_value() == "hf_test_token_1234567890abcdef"

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
        creds_from_file = HuggingFaceHubCredentials.read(str(HF_HUB_CREDENTIALS_PATH))
        assert isinstance(creds_from_file.token, SecretStr), (
            "token should be a SecretStr instance"
        )
        assert (
            creds_from_file.token.get_secret_value() == "hf_test_token_1234567890abcdef"
        )

        # Test loading from environment variable
        monkeypatch.setenv("HF_TOKEN", "hf_env_token_abcdef1234567890")
        creds_from_env = HuggingFaceHubCredentials.read()
        assert isinstance(creds_from_env.token, SecretStr), (
            "token should be a SecretStr instance"
        )
        assert (
            creds_from_env.token.get_secret_value() == "hf_env_token_abcdef1234567890"
        )

        # Test error raised when env var is not set and no path provided
        monkeypatch.delenv("HF_TOKEN", raising=False)
        with pytest.raises(MissingHFTokenError):
            HuggingFaceHubCredentials.read()
