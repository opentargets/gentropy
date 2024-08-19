"""Test colocalisation methods."""

from __future__ import annotations

from typing import Any

import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ECaviar


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


def test_coloc_no_logbf(
    spark: SparkSession,
    minimum_expected_h0: float = 0.99,
    maximum_expected_h4: float = 1e-5,
) -> None:
    """Test COLOC output when the input data has irrelevant logBF."""
    observed_overlap = StudyLocusOverlap(
        (
            spark.createDataFrame(
                [
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 2,
                        "chromosome": "1",
                        "tagVariantId": "snp",
                        "statistics": {
                            "left_logBF": None,
                            "right_logBF": None,
                        },  # irrelevant for COLOC
                    }
                ],
                schema=StructType(
                    [
                        StructField("leftStudyLocusId", LongType(), False),
                        StructField("rightStudyLocusId", LongType(), False),
                        StructField("chromosome", StringType(), False),
                        StructField("tagVariantId", StringType(), False),
                        StructField(
                            "statistics",
                            StructType(
                                [
                                    StructField("left_logBF", DoubleType(), True),
                                    StructField("right_logBF", DoubleType(), True),
                                ]
                            ),
                        ),
                    ]
                ),
            )
        ),
        StudyLocusOverlap.get_schema(),
    )
    observed_coloc_df = Coloc.colocalise(observed_overlap).df
    assert (
        observed_coloc_df.select("h0").collect()[0]["h0"] > minimum_expected_h0
    ), "COLOC should return a high h0 (no association) when the input data has irrelevant logBF."
    assert (
        observed_coloc_df.select("h4").collect()[0]["h4"] < maximum_expected_h4
    ), "COLOC should return a low h4 (traits are associated) when the input data has irrelevant logBF."


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)
