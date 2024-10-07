"""Test PICS finemapping."""

from __future__ import annotations

import pyspark.sql.functions as f
from pyspark.sql import Row

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.pics import PICS


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
        assert observed_df.collect()[0]["locus"] == []

    def test_finemap_null_ld_set(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test how we apply `finemap` when `ldSet` is null by returning a null field."""
        mock_study_locus.df = mock_study_locus.df.filter(f.col("ldSet").isNull())
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        assert observed_df.collect()[0]["locus"] is None

    def test_finemap_quality_control(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test that we add a `empty locus` flag when any variant in the locus meets PICS criteria."""
        mock_study_locus.df = mock_study_locus.df.withColumn(
            # Association with an empty ldSet
            "ldSet",
            f.when(f.col("ldSet").isNull(), f.array()).otherwise(f.col("ldSet")),
        ).filter(f.size("ldSet") == 0)
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        qc_flag = "LD block does not contain variants at the required R^2 threshold"
        assert (
            qc_flag in observed_df.collect()[0]["qualityControls"]
        ), "Empty locus QC flag is missing."


def test__finemap_udf() -> None:
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
            "standardError": 0.07420896512708416,
            "posteriorProbability": 0.07116959886882368,
        },
        {
            "variantId": "var2",
            "r2Overall": 1,
            "standardError": 0.9977000638225533,
            "posteriorProbability": 0.9288304011311763,
        },
    ]
    for idx, tag in enumerate(result):  # type: ignore
        # assert both dictionaries have the same content regardless of its order
        assert tag == expected[idx]


def test_finemap(mock_study_locus: StudyLocus) -> None:
    """Test finemap function returns study-locus."""
    assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)
