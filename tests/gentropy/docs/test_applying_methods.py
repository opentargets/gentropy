"""Testing applying methods docs."""

from typing import Any

import pytest
from pyspark.sql import SparkSession

from docs.src_snippets.howto.python_api.c_applying_methods import (
    apply_class_method_clumping,
    apply_class_method_pics,
    apply_instance_method,
)
from gentropy.dataset.credible_set import CredibleSet
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import LDAnnotatedStudyLocus, StudyLocus
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
    spark: SparkSession,
    mock_study_index: StudyIndex,
    mock_summary_statistics: SummaryStatistics,
) -> None:
    """Test the methods in applying_methods return the expected dataset."""
    if func == apply_class_method_clumping:
        assert isinstance(func(mock_summary_statistics), StudyLocus)
    elif func == apply_instance_method:
        assert isinstance(func(mock_summary_statistics), SummaryStatistics)
    elif func == apply_class_method_pics:
        locus = [("1_100_A_T", 1.0, -10, 0.3, 1000, 0.1, 0.01)]
        loci = LDAnnotatedStudyLocus(
            spark.createDataFrame(
                [
                    (
                        "L1",
                        "S1",
                        "1",
                        1,
                        1000,
                        "1_100_A_T",
                        [],
                        locus,
                        [("1_100_A_T", 1.0)],
                    )
                ],
                LDAnnotatedStudyLocus.get_schema(),
            ),
            LDAnnotatedStudyLocus.get_schema(),
        )
        assert isinstance(func(loci, mock_study_index), CredibleSet)
