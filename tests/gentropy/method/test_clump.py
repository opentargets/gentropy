"""Test clumping methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest

from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.clump import LDclumping

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test PICS."""
    assert isinstance(LDclumping.clump(mock_study_locus), StudyLocus)


class TestIsLeadLinked:
    """Testing the is_lead_linked method."""

    DATA = [
        # Linked to V2:
        (
            "s1",
            1,
            "c1",
            "v3",
            1.0,
            -8,
            [{"tagVariantId": "v3"}, {"tagVariantId": "v2"}, {"tagVariantId": "v4"}],
            True,
        ),
        # True lead:
        (
            "s1",
            2,
            "c1",
            "v1",
            1.0,
            -10,
            [{"tagVariantId": "v1"}, {"tagVariantId": "v2"}, {"tagVariantId": "v3"}],
            False,
        ),
        # Linked to V1:
        (
            "s1",
            3,
            "c1",
            "v2",
            1.0,
            -9,
            [{"tagVariantId": "v2"}, {"tagVariantId": "v1"}],
            True,
        ),
        # Independent - No LD set:
        ("s1", 4, "c1", "v10", 1.0, -10, [], False),
        # Independent - No variantId:
        ("s1", 5, "c1", None, 1.0, -10, [], False),
        # An other independent variant on the same chromosome, but lead is not in ldSet:
        (
            "s1",
            6,
            "c1",
            "v6",
            1.0,
            -8,
            [{"tagVariantId": "v7"}, {"tagVariantId": "v8"}, {"tagVariantId": "v9"}],
            False,
        ),
        # An other independent variant on a different chromosome, but lead is not in ldSet:
        (
            "s1",
            7,
            "c2",
            "v10",
            1.0,
            -8,
            [{"tagVariantId": "v2"}, {"tagVariantId": "v10"}],
            False,
        ),
    ]

    SCHEMA = t.StructType(
        [
            t.StructField("studyId", t.StringType(), True),
            t.StructField("studyLocusId", t.StringType(), True),
            t.StructField("chromosome", t.StringType(), True),
            t.StructField("variantId", t.StringType(), True),
            t.StructField("pValueMantissa", t.FloatType(), True),
            t.StructField("pValueExponent", t.IntegerType(), True),
            t.StructField(
                "ldSet",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("tagVariantId", t.StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            t.StructField("expected_flag", t.BooleanType(), True),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestIsLeadLinked, spark: SparkSession) -> None:
        """Setup study the mock index for testing."""
        # Store input data:
        self.df = spark.createDataFrame(self.DATA, self.SCHEMA)

    def test_is_lead_correctness(self: TestIsLeadLinked) -> None:
        """Test the correctness of the is_lead_linked method."""
        observed = self.df.withColumn(
            "is_lead_linked",
            LDclumping._is_lead_linked(
                f.col("studyId"),
                f.col("chromosome"),
                f.col("variantId"),
                f.col("pValueExponent"),
                f.col("pValueMantissa"),
                f.col("ldSet"),
            ),
        ).collect()

        for row in observed:
            assert row["is_lead_linked"] == row["expected_flag"]

    def test_flagging(self: TestIsLeadLinked) -> None:
        """Test flagging of lead variants."""
        # Create the study locus and clump:
        sl_flagged = StudyLocus(
            _df=self.df.drop("expected_flag").withColumn(
                "qualityControls", f.array().cast("array<string>")
            ),
            _schema=StudyLocus.get_schema(),
        ).clump()

        # Assert that the clumped locus is a StudyLocus:
        assert isinstance(sl_flagged, StudyLocus)

        # Assert that the clumped locus has the correct columns:
        for row in sl_flagged.df.join(self.df, on="studylocusId").collect():
            if len(row["qualityControls"]) == 0:
                assert not row["expected_flag"]
            else:
                assert row["expected_flag"]
