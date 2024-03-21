"""Test colocalisation methods."""

from __future__ import annotations

from typing import Any

import pytest
from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ECaviar
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession


def test_coloc(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test coloc."""
    assert isinstance(Coloc.colocalise(mock_study_locus_overlap), Colocalisation)


@pytest.mark.parametrize(
    "observed_data, expected_data",
    [
        # associations with a single overlapping SNP
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "snp",
                    "statistics": {"left_logBF": 10.3, "right_logBF": 10.5},
                },
            ],
            # expected coloc
            [
                {
                    "h0": 9.254841951638903e-5,
                    "h1": 2.7517068829182966e-4,
                    "h2": 3.3609423764447284e-4,
                    "h3": 9.254841952564387e-13,
                    "h4": 0.9992961866536217,
                },
            ],
        ),
        # associations with multiple overlapping SNPs
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {"left_logBF": 10.3, "right_logBF": 10.5},
                },
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {"left_logBF": 10.3, "right_logBF": 10.5},
                },
            ],
            # expected coloc
            [
                {
                    "h0": 4.6230151407950416e-5,
                    "h1": 2.749086942648107e-4,
                    "h2": 3.357742374172504e-4,
                    "h3": 9.983447421747411e-4,
                    "h4": 0.9983447421747356,
                },
            ],
        ),
    ],
)
def test_coloc_semantic(
    spark: SparkSession,
    observed_data: list[Any],
    expected_data: list[Any],
) -> None:
    """Test our COLOC with the implementation in R."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(observed_data, schema=StudyLocusOverlap.get_schema()),
        _schema=StudyLocusOverlap.get_schema(),
    )
    observed_coloc_pdf = (
        Coloc.colocalise(observed_overlap)
        .df.select("h0", "h1", "h2", "h3", "h4")
        .toPandas()
    )
    expected_coloc_pdf = (
        spark.createDataFrame(expected_data)
        .select("h0", "h1", "h2", "h3", "h4")
        .toPandas()
    )

    assert_frame_equal(
        observed_coloc_pdf,
        expected_coloc_pdf,
        check_exact=False,
        check_dtype=True,
    )


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)
