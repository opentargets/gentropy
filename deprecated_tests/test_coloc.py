"""Tests on helper functions that find overlapping signals between credible sets."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
import pytest
from pandas.testing import assert_frame_equal

from otg.coloc.coloc import ecaviar_colocalisation
from otg.coloc.overlaps import find_gwas_vs_all_overlapping_peaks
from otg.coloc.utils import _extract_credible_sets

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def gwas_vs_all_overlap_schema() -> t.StructType:
    """Schema of the gwas_vs_all overlap output dataframe."""
    return t.StructType(
        [
            t.StructField("left_chromosome", t.StringType(), False),
            t.StructField("left_studyId", t.StringType(), False),
            t.StructField("left_leadVariantId", t.StringType(), False),
            t.StructField("left_type", t.StringType(), True),
            t.StructField("left_posteriorProbability", t.DoubleType(), False),
            t.StructField("right_chromosome", t.StringType(), True),
            t.StructField("right_studyId", t.StringType(), True),
            t.StructField("right_leadVariantId", t.StringType(), True),
            t.StructField("right_type", t.StringType(), True),
            t.StructField("right_posteriorProbability", t.DoubleType(), True),
            t.StructField("tagVariantId", t.StringType(), True),
        ]
    )


class TestFindGwasVsAllOverlappingPeaks:
    """Tests on find_gwas_vs_all_overlapping_peaks."""

    test_input = [
        [
            # Overlapping peak between two GWAS
            ("22", "GCST90002383", "22_28449893_G_C", "22_28026789_C_T", 0.2, "gwas"),
            ("22", "GCST90002310", "22_28690105_C_T", "22_28026789_C_T", 0.1, "gwas"),
        ],
        [
            # Overlapping peak between a GWAS and a eQTL
            ("22", "GCST90002383", "22_28449893_G_C", "22_28026789_C_T", 0.2, "gwas"),
            ("22", "eABC123", "22_28690105_C_T", "22_28026789_C_T", 0.3, "eqtl"),
        ],
        [
            # Overlapping peak between a pQTL and a eQTL
            ("22", "pABC123", "22_28449893_G_C", "22_28026789_C_T", 0.4, "pqtl"),
            ("22", "eABC123", "22_28690105_C_T", "22_28026789_C_T", 0.3, "eqtl"),
        ],
    ]

    expected_output = [
        [
            # Overlapping peak between two GWAS
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                0.2,
                "22",
                "GCST90002310",
                "22_28690105_C_T",
                "gwas",
                0.1,
                "22_28026789_C_T",
            ),
        ],
        [
            # Overlapping peak between a GWAS and a eQTL
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                0.2,
                "22",
                "eABC123",
                "22_28690105_C_T",
                "eqtl",
                0.3,
                "22_28026789_C_T",
            ),
        ],
        [
            # Overlapping peak between a pQTL and a eQTL should not be returned
        ],
    ]

    @pytest.mark.parametrize(
        ("test_input", "expected_output"), zip(test_input, expected_output)
    )
    def test_find_gwas_vs_all_overlapping_peaks(
        self: TestFindGwasVsAllOverlappingPeaks,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        gwas_vs_all_overlap_schema: t.StructType,
    ) -> None:
        """Test that find_gwas_vs_all_overlapping_peaks returns the expected DataFrame."""
        mock_df = spark.createDataFrame(
            data=test_input,
            schema=[
                "chromosome",
                "studyId",
                "leadVariantId",
                "tagVariantId",
                "posteriorProbability",
                "type",
            ],
        )
        test_df = find_gwas_vs_all_overlapping_peaks(
            mock_df, causality_statistic="posteriorProbability"
        ).toPandas()
        expected_df = spark.createDataFrame(
            data=expected_output,
            schema=gwas_vs_all_overlap_schema,
        ).toPandas()

        assert_frame_equal(test_df, expected_df)


class TestEcaviarColocalisation:
    """Tests on ecaviar_colocalisation."""

    test_input = [
        # Overlapping peak between two GWAS with a significant CLPP (> 0.001)
        [
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                0.2,
                "22",
                "GCST90002310",
                "22_28690105_C_T",
                "gwas",
                0.1,
                "22_28026789_C_T",
            )
        ],
        [
            # Overlapping peak between two GWAS with a subsignificant CLPP (< 0.001)
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                0.02,
                "22",
                "GCST90002310",
                "22_28690105_C_T",
                "gwas",
                0.01,
                "22_28026789_C_T",
            )
        ],
    ]

    expected_output = [
        [
            (
                "22",
                "GCST90002383",
                "22_28449893_G_C",
                "gwas",
                "22",
                "GCST90002310",
                "22_28690105_C_T",
                "gwas",
                1,
                0.02,
            )
        ],
        [],
    ]

    @pytest.fixture(scope="class")
    def ecaviar_coloc_output_schema(self: TestEcaviarColocalisation) -> t.StructType:
        """Schema of the gwas_vs_all overlap output dataframe."""
        return t.StructType(
            [
                t.StructField("left_chromosome", t.StringType(), False),
                t.StructField("left_studyId", t.StringType(), False),
                t.StructField("left_leadVariantId", t.StringType(), False),
                t.StructField("left_type", t.StringType(), True),
                t.StructField("right_chromosome", t.StringType(), True),
                t.StructField("right_studyId", t.StringType(), True),
                t.StructField("right_leadVariantId", t.StringType(), True),
                t.StructField("right_type", t.StringType(), True),
                t.StructField("coloc_n_vars", t.LongType(), False),
                t.StructField("clpp", t.DoubleType(), True),
            ]
        )

    @pytest.mark.parametrize(
        ("test_input", "expected_output"), zip(test_input, expected_output)
    )
    def test_ecaviar_colocalisation(
        self: TestEcaviarColocalisation,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        gwas_vs_all_overlap_schema: t.StructType,
        ecaviar_coloc_output_schema: t.StructType,
    ) -> None:
        """Test that ecaviar_colocalisation returns the expected DataFrame."""
        mock_df = spark.createDataFrame(
            data=test_input,
            schema=gwas_vs_all_overlap_schema,
        )
        test_df = (
            ecaviar_colocalisation(mock_df, clpp_threshold=0.001)
            .toPandas()
            .dropna(axis=1, how="all")
        )
        expected_df = (
            spark.createDataFrame(
                data=expected_output,
                schema=ecaviar_coloc_output_schema,
            )
            .toPandas()
            .dropna(axis=1, how="all")
        )

        assert_frame_equal(test_df, expected_df)


class TestExtractCredibleSets:
    """Tests the function that extracts the credible sets in a study/locus."""

    test_input = [
        [
            # Example with credible sets from all methods
            (
                "10_100346008_G_A",
                "7232622490995985132",
                [
                    {
                        "method": "conditional",
                        "credibleSet": [
                            {
                                "is95CredibleSet": True,
                                "tagVariantId": "10_101278237_A_G",
                                "logABF": 1.59,
                                "posteriorProbability": None,
                            }
                        ],
                    },
                    {
                        "method": "SuSIE",
                        "credibleSet": [
                            {
                                "is95CredibleSet": True,
                                "tagVariantId": "10_101278237_A_G",
                                "logABF": None,
                                "posteriorProbability": 0.001,
                            }
                        ],
                    },
                    {
                        "method": "pics",
                        "credibleSet": [
                            {
                                "is95CredibleSet": True,
                                "tagVariantId": "10_101278237_A_G",
                                "logABF": None,
                                "posteriorProbability": 0.001,
                            }
                        ],
                    },
                ],
            )
        ],
        [
            # Example with a not confident enouth credible set
            (
                "10_100346008_G_A",
                "7232622490995985132",
                [
                    {
                        "method": "conditional",
                        "credibleSet": [
                            {
                                "is95CredibleSet": False,
                                "tagVariantId": "10_101278237_A_G",
                                "logABF": 1.59,
                                "posteriorProbability": None,
                            }
                        ],
                    }
                ],
            )
        ],
    ]

    expected_output = [
        [
            (
                "10_100346008_G_A",
                "7232622490995985132",
                "10_101278237_A_G",
                1.59,
                None,
                "10",
                "conditional",
            ),
            (
                "10_100346008_G_A",
                "7232622490995985132",
                "10_101278237_A_G",
                None,
                0.001,
                "10",
                "SuSIE",
            ),
            (
                "10_100346008_G_A",
                "7232622490995985132",
                "10_101278237_A_G",
                None,
                0.001,
                "10",
                "pics",
            ),
        ],
        [
            # Example with a not confident enouth credible set won't be picked up
        ],
    ]

    @pytest.fixture(scope="class")
    def study_locus_schema(self: TestExtractCredibleSets) -> t.StructType:
        """Schema of the study_locus dataframe."""
        return t.StructType(
            [
                t.StructField("variantId", t.StringType(), True),
                t.StructField("studyId", t.StringType(), True),
                t.StructField(
                    "credibleSets",
                    t.ArrayType(
                        t.StructType(
                            [
                                t.StructField("method", t.StringType(), True),
                                t.StructField(
                                    "credibleSet",
                                    t.ArrayType(
                                        t.StructType(
                                            [
                                                t.StructField(
                                                    "is95CredibleSet",
                                                    t.BooleanType(),
                                                    True,
                                                ),
                                                t.StructField(
                                                    "logABF", t.DoubleType(), True
                                                ),
                                                t.StructField(
                                                    "posteriorProbability",
                                                    t.DoubleType(),
                                                    True,
                                                ),
                                                t.StructField(
                                                    "tagVariantId", t.StringType(), True
                                                ),
                                            ]
                                        )
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                ),
            ]
        )

    @pytest.fixture(scope="class")
    def credible_sets_schema(self: TestExtractCredibleSets) -> t.StructType:
        """Schema of the credible sets df after extracting it from the study locus df."""
        return t.StructType(
            [
                t.StructField("leadVariantId", t.StringType(), False),
                t.StructField("studyId", t.StringType(), False),
                t.StructField("tagVariantId", t.StringType(), False),
                t.StructField("logABF", t.DoubleType(), True),
                t.StructField("posteriorProbability", t.DoubleType(), True),
                t.StructField("chromosome", t.StringType(), False),
                t.StructField("method", t.StringType(), False),
            ]
        )

    @pytest.mark.parametrize(
        ("test_input", "expected_output"), zip(test_input, expected_output)
    )
    def test_extract_credible_sets(
        self: TestExtractCredibleSets,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        study_locus_schema: t.StructType,
        credible_sets_schema: t.StructType,
    ) -> None:
        """Test that extract_credible_sets returns the expected DataFrame."""
        mock_df = spark.createDataFrame(
            data=test_input,
            schema=study_locus_schema,
        )
        test_df = _extract_credible_sets(mock_df).toPandas().dropna(axis=1, how="all")
        expected_df = (
            spark.createDataFrame(data=expected_output, schema=credible_sets_schema)
            .toPandas()
            .dropna(axis=1, how="all")
        )

        assert_frame_equal(test_df, expected_df)
