"""Test study validation step."""

import pytest
from pydantic import ValidationError

from gentropy.study_validation import StudyValidationDefaults, StudyValidationStep


class TestStudyValidationDefaults:
    """Test StudyValidationDefaults config validation."""

    def test_rejects_missing_required(self) -> None:
        """Test that StudyValidationDefaults rejects missing required fields."""
        with pytest.raises(ValidationError):
            StudyValidationDefaults(
                valid_study_index_path="/tmp/valid",  # noqa: S108
                invalid_study_index_path="/tmp/invalid",  # noqa: S108
            )

    def test_accepts_all_required(self) -> None:
        """Test that StudyValidationDefaults accepts all required fields with optional defaults."""
        config = StudyValidationDefaults(
            study_index_path=["/tmp/study"],  # noqa: S108
            target_index_path="/tmp/target",  # noqa: S108
            disease_index_path="/tmp/disease",  # noqa: S108
            biosample_index_path="/tmp/biosample",  # noqa: S108
            valid_study_index_path="/tmp/valid",  # noqa: S108
            invalid_study_index_path="/tmp/invalid",  # noqa: S108
        )
        assert config.invalid_qc_reasons == []
        assert config.deprecated_project_ids is None


class TestStudyValidationStep:
    """Test StudyValidationStep parameter pattern."""

    def test_step_parameters(self) -> None:
        """Test that StudyValidationStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(StudyValidationStep.__init__)
        params = list(sig.parameters.keys())

        assert "self" in params
        assert "config" in params
        assert "session" in params
