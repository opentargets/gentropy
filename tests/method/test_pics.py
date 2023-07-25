"""Test PICS finemapping."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Row

from otg.dataset.study_locus import StudyLocus
from otg.method.pics import PICS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType


class TestFinemap:
    """Test PICS finemap function under different scenarios."""

    def test_finemap_pipeline(self: TestFinemap, mock_study_locus: StudyLocus) -> None:
        """Test finemap works with a mock study locus."""
        assert isinstance(PICS.finemap(mock_study_locus), StudyLocus)

    def test_finemap_empty_array(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test finemap works when `credibleSet` is an empty array by returning an empty array."""
        mock_study_locus.df = mock_study_locus.df.withColumn(
            "ldSet",
            # empty array following the `credibleSet` schema
            f.when(f.col("ldSet").isNull(), f.array()).otherwise(f.col("ldSet")),
        ).filter(f.size("ldSet") == 0)
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        print("TEST_FINEMAP_EMPTY_ARRAY", observed_df.show(truncate=False))
        assert observed_df.collect()[0]["credibleSet"] == []

    def test_finemap_null_ld_set(
        self: TestFinemap, mock_study_locus: StudyLocus
    ) -> None:
        """Test how we apply `finemap` when `credibleSet` is null by returning a null field."""
        mock_study_locus.df = mock_study_locus.df.filter(f.col("ldSet").isNull())
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        print("TEST_FINEMAP_NULL", observed_df.show(truncate=False))
        assert observed_df.collect()[0]["credibleSet"] is None

    def test_finemap_null_r2(
        self: TestFinemap, spark: SparkSession, study_locus_schema: StructType
    ) -> None:
        """Test finemap works when `r2Overall` is null by returning the same `credibleSet` content."""
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
                mock_study_locus_null_r2_data, schema=study_locus_schema
            )
        )
        observed_df = PICS.finemap(mock_study_locus).df.limit(1)
        # since PICS can't be run, it returns the same content
        print("TEST_FINEMAP_EMPTY_R2", observed_df.show(truncate=False))
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
            "tagPValue": 1e-08,
            "tagStandardError": 0.8294246485510745,
            "posteriorProbability": 7.068873779583866e-134,
        },
        {
            "variantId": "var2",
            "r2Overall": 1,
            "tagPValue": 1e-10,
            "tagStandardError": 0.9977000638225533,
            "posteriorProbability": 1.0,
        },
    ]
    for idx, tag in enumerate(result):  # type: ignore
        # assert both dictionaries have the same content regardless of its order
        assert tag == expected[idx]
