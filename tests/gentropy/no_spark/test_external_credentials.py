"""Tests for external vendor credential models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from gentropy.external.hf_hub.credentials import HuggingFaceHubCredentials
from gentropy.external.wandb.credentials import WandbCredentials

DATA_SAMPLES = Path(__file__).parent.parent / "data_samples"
WANDB_CREDENTIALS_PATH = DATA_SAMPLES / "wandb_credentials.json"
HF_HUB_CREDENTIALS_PATH = DATA_SAMPLES / "hf_hub_credentials.json"


class TestWandbCredentials:
    """Tests for WandbCredentials model."""

    def test_from_json_loads_api_key(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = WandbCredentials.from_json(str(WANDB_CREDENTIALS_PATH))
        assert creds.api_key == "test_wandb_api_key_1234567890abcdef"

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
        with pytest.raises(ValueError):  # noqa: PT011
            WandbCredentials.from_json(str(bad_file))


class TestHuggingFaceHubCredentials:
    """Tests for HuggingFaceHubCredentials model."""

    def test_from_json_loads_token(self) -> None:
        """Credentials are loaded correctly from the sample JSON file."""
        creds = HuggingFaceHubCredentials.from_json(str(HF_HUB_CREDENTIALS_PATH))
        assert creds.token == "hf_test_token_1234567890abcdef"

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
        with pytest.raises(ValueError):  # noqa: PT011
            HuggingFaceHubCredentials.from_json(str(bad_file))


class TestLocusToGeneStepHfTokenResolution:
    """Tests for LocusToGeneStep._get_hf_token() credential resolution."""

    def _make_step(self, **kwargs):  # type: ignore[no-untyped-def]
        """Return a minimal LocusToGeneStep-like namespace without running Spark."""
        from types import SimpleNamespace

        from gentropy.l2g import LocusToGeneStep

        # Bind _get_hf_token unbound method to a plain namespace so we can
        # test its resolution logic without spinning up Spark or loading data.
        ns = SimpleNamespace(
            download_from_hub=True,
            hf_hub_credentials_path=None,
            **kwargs,
        )
        ns._get_hf_token = LocusToGeneStep._get_hf_token.__get__(ns)
        return ns

    def test_returns_token_from_credentials_file(self) -> None:
        """Token is read from the JSON credentials file when path is provided."""
        step = self._make_step()
        step.hf_hub_credentials_path = str(HF_HUB_CREDENTIALS_PATH)
        assert step._get_hf_token() == "hf_test_token_1234567890abcdef"

    def test_returns_none_when_download_from_hub_false(self) -> None:
        """None is returned when download_from_hub is False regardless of credentials."""
        step = self._make_step()
        step.download_from_hub = False
        assert step._get_hf_token() is None

    def test_returns_env_token_when_no_credentials_file(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """HF_TOKEN env variable is used as fallback when no credentials file is set."""
        monkeypatch.setenv("HF_TOKEN", "env_hf_token_abc")
        step = self._make_step()
        assert step._get_hf_token() == "env_hf_token_abc"

    def test_raises_when_no_credentials_and_no_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ValueError is raised when download_from_hub=True but no token is available."""
        monkeypatch.delenv("HF_TOKEN", raising=False)
        step = self._make_step()
        with pytest.raises(ValueError, match="hf_hub_credentials_path"):
            step._get_hf_token()
