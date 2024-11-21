"""Test PICS finemapping."""

from __future__ import annotations

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import Row, SparkSession

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


def test__finemap_udf() -> None:
    """Test the _finemap UDF with a simple case."""
    ld_set = [
        Row(tagVariantId="var1", r2Overall=0.8),
        Row(tagVariantId="var2", r2Overall=1),
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

    assert result is not None, "The result of _finemap should not be None"
    for idx, tag in enumerate(result):
        # assert both dictionaries have the same content regardless of its order
        assert tag == expected[idx]


def test_finemap(mock_study_locus: StudyLocus) -> None:
    """Test finemap function returns study-locus."""
    assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)


class TestLeadPropagation:
    """This test suite is designed to test that the statistics of the lead variant are propagated correctly."""

    DATA = [
        ("v1", "v1", 1.0),
        ("v1", "v2", 0.9),
        ("v1", "v3", 0.3),
    ]

    @pytest.fixture(autouse=True)
    def setup(self, spark: SparkSession) -> None:
        """Set up the test suite.

        Args:
            spark (SparkSession): The spark session.
        """
        df = (
            spark.createDataFrame(self.DATA, ["variantId", "tagVariantId", "r2Overall"])
            .groupBy("variantId")
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("tagVariantId"),
                        f.col("r2Overall").cast(t.DoubleType()).alias("r2Overall"),
                    )
                ).alias("ldSet")
            )
            .withColumns(
                {
                    "studyLocusId": f.lit("l1"),
                    "studyId": f.lit("s1"),
                    "chromosome": f.lit("1"),
                    "pValueMantissa": f.lit(1.0).cast(t.FloatType()),
                    "pValueExponent": f.lit(-4).cast(t.IntegerType()),
                    "beta": f.lit(0.234).cast(t.DoubleType()),
                    "qualityControls": f.lit(None).cast(t.ArrayType(t.StringType())),
                    "ldSet": f.filter(
                        f.col("ldSet"), lambda x: x.tagVariantId.isNotNull()
                    ),
                }
            )
        )

        self.study_locus = StudyLocus(_df=df, _schema=StudyLocus.get_schema())

    def test_lead_propagation(self: TestLeadPropagation) -> None:
        """Testing if all the lead variant statistics are propagated to the tag variants."""
        # Explode all the tags:
        finemapped = (
            PICS.finemap(self.study_locus)
            .df.select(
                f.col("variantId"),
                f.col("pValueMantissa"),
                f.col("pValueExponent"),
                f.col("beta"),
                f.explode("locus").alias("locus"),
            )
            .collect()
        )

        # Looping through all the tags and checking if the statistics are propagated correctly:
        for row in finemapped:
            if row["locus"]["variantId"] == row["variantId"]:
                assert row["locus"]["pValueMantissa"] == row["pValueMantissa"]
                assert row["locus"]["pValueExponent"] == row["pValueExponent"]
                assert row["locus"]["beta"] == row["beta"]
            else:
                assert row["locus"]["pValueMantissa"] is None
                assert row["locus"]["pValueExponent"] is None
                assert row["locus"]["beta"] is None
