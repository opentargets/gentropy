"""Test study locus validation step."""

import pytest
from pydantic import ValidationError

from gentropy.study_locus_validation import (
    StudyLocusValidationDefaults,
    StudyLocusValidationStep,
)


class TestStudyLocusValidationDefaults:
    """Test StudyLocusValidationDefaults config validation."""

    def test_rejects_missing_required(self) -> None:
        """Test that StudyLocusValidationDefaults rejects missing required fields."""
        with pytest.raises(ValidationError):
            StudyLocusValidationDefaults(
                valid_study_locus_path="/tmp/valid",  # noqa: S108
                invalid_study_locus_path="/tmp/invalid",  # noqa: S108
                trans_qtl_threshold=1000000,
            )

    def test_accepts_all_required(self) -> None:
        """Test that StudyLocusValidationDefaults accepts all required fields with optional defaults."""
        config = StudyLocusValidationDefaults(
            study_locus_path=["/tmp/study_locus"],  # noqa: S108
            study_index_path="/tmp/study_index",  # noqa: S108
            target_index_path="/tmp/target",  # noqa: S108
            valid_study_locus_path="/tmp/valid",  # noqa: S108
            invalid_study_locus_path="/tmp/invalid",  # noqa: S108
            trans_qtl_threshold=1000000,
        )
        assert config.invalid_qc_reasons == []


class TestStudyLocusValidationStep:
    """Test StudyLocusValidationStep parameter pattern."""

    def test_step_parameters(self) -> None:
        """Test that StudyLocusValidationStep has correct expected parameters."""
        import inspect

        sig = inspect.signature(StudyLocusValidationStep.__init__)
        params = list(sig.parameters.keys())

        assert "self" in params
        assert "config" in params
        assert "session" in params
