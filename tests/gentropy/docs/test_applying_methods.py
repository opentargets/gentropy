"""Testing applying methods docs."""

from typing import Any

import pytest

from docs.src_snippets.howto.python_api.c_applying_methods import (
    apply_class_method_clumping,
    apply_class_method_pics,
    apply_instance_method,
)
from gentropy.dataset.study_locus_view import StudyLocusView
from gentropy.dataset.summary_statistics import SummaryStatistics


@pytest.mark.parametrize(
    "func",
    [
        apply_class_method_clumping,
        apply_class_method_pics,
        apply_instance_method,
    ],
)
def test_apply_methods(
    func: Any,
    mock_study_locus: StudyLocusView,
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test any method in applying_methods returns an instance of StudyLocusView."""
    if func in [apply_class_method_clumping, apply_instance_method]:
        assert isinstance(func(mock_summary_statistics), StudyLocusView)
    elif func == apply_class_method_pics:
        assert isinstance(func(mock_study_locus), StudyLocusView)
