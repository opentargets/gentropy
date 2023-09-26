"""Test PICS finemapping."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Row

from otg.dataset.study_locus import StudyLocus
from otg.method.pics import PICS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestFinemap:
    """Test PICS finemap function under different scenarios."""

    def test_finemap_pipeline(self: TestFinemap, mock_study_locus: StudyLocus) -> None:
        """Test finemap works with a mock study locus."""
        assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)

    def test_finemap_empty_array(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test finemap works when `locus` is an empty array by returning an empty array."""
        mock_study_locus.df = mock_study_locus.df.withColumn(
            "ldSet",
            # empty array following the `locus` schema
            f.when(f.col("ldSet").isNull(), f.array()).otherwise(f.col("ldSet")),
        ).filter(f.size("ldSet") == 0)
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        print("TEST_FINEMAP_EMPTY_ARRAY", observed_df.show(truncate=False))
        assert observed_df.collect()[0]["locus"] == []

    def test_finemap_null_ld_set(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test how we apply `finemap` when `locus` is null by returning a null field."""
        mock_study_locus.df = mock_study_locus.df.filter(f.col("ldSet").isNull())
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        print("TEST_FINEMAP_NULL", observed_df.show(truncate=False))
        assert observed_df.collect()[0]["locus"] is None

    def test_finemap_null_r2(
        self: TestFinemap,
        spark: SparkSession,
    ) -> None:
        """Test finemap works when `r2Overall` is null by returning the same `locus` content."""
        mock_study_locus_null_r2_data: list = [
            (
                1,
                "varA",
                "chr1",
                10,
                "studyA",
                None,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                5.0,
                -8,
                0.0,
                0.0,
                "0",
                [],
                "pics",
                [
                    (
                        "tagA",
                        None,  # null r2
                    )
                ],
                None,
            )
        ]

        mock_study_locus = StudyLocus(
            _df=spark.createDataFrame(
                mock_study_locus_null_r2_data, schema=StudyLocus.get_schema()
            ),
            _schema=StudyLocus.get_schema(),
        )
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        # since PICS can't be run, it returns the same content
        assert observed_df.collect()[0] == mock_study_locus.df.collect()[0]


def test__finemap() -> None:
    """Test the _finemap UDF with a simple case."""
    ld_set = [
        Row(variantId="var1", r2Overall=0.8),
        Row(variantId="var2", r2Overall=1),
    ]
    result = PICS._finemap(ld_set, lead_neglog_p=10.0, k=6.4)
    expected = [
        {
            "variantId": "var1",
            "r2Overall": 0.8,
            "pValueMantissa": 1.0,
            "pValueExponent": -8,
            "standardError": 0.07420896512708416,
            "posteriorProbability": 0.07116959886882368,
        },
        {
            "variantId": "var2",
            "r2Overall": 1,
            "pValueMantissa": 1.0,
            "pValueExponent": -10,
            "standardError": 0.9977000638225533,
            "posteriorProbability": 0.9288304011311763,
        },
    ]
    for idx, tag in enumerate(result):  # type: ignore
        # assert both dictionaries have the same content regardless of its order
        assert tag == expected[idx]
