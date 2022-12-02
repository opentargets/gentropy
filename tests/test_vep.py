"""Tests on helper functions that extract V2G assignments from VEP."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pandas.testing import assert_frame_equal

from etl.v2g.functional_predictions.vep import (
    get_plof_flag,
    get_polyphen_score,
    get_sift_score,
    get_variant_consequences,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="module")
def variant_df_schema() -> t.StructType:
    """Schema of the mock VEP dataframe."""
    return t.StructType(
        [
            t.StructField("variantId", t.StringType(), False),
            t.StructField("chromosome", t.StringType(), False),
            t.StructField("gene_id", t.StringType(), False),
            t.StructField("consequence_terms", t.ArrayType(t.StringType()), True),
            t.StructField("polyphen_score", t.DoubleType(), True),
            t.StructField("polyphen_prediction", t.StringType(), True),
            t.StructField("sift_score", t.DoubleType(), True),
            t.StructField("sift_prediction", t.StringType(), True),
            t.StructField("lof", t.StringType(), True),
        ]
    )


@pytest.fixture(scope="module")
def v2g_df_schema() -> t.StructType:
    """Schema of the V2G dataframe."""
    return t.StructType(
        [
            t.StructField("variantId", t.StringType(), False),
            t.StructField("chromosome", t.StringType(), False),
            t.StructField("geneId", t.StringType(), False),
            t.StructField("variantFunctionalConsequenceId", t.StringType(), True),
            t.StructField("label", t.StringType(), True),
            t.StructField("isHighQualityPlof", t.BooleanType(), True),
            t.StructField("score", t.DoubleType(), True),
            t.StructField("datatypeId", t.StringType(), True),
            t.StructField("datasourceId", t.StringType(), True),
        ]
    )


class TestGetVariantConsequences:
    """Test the get_variant_consequences util."""

    test_input = [
        [
            # High impact
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                ["stop_gained"],
                None,
                None,
                None,
                None,
                None,
            )
        ],
        [
            # Low impact
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                ["splice_region_variant", "5_prime_UTR_variant"],
                None,
                None,
                None,
                None,
                None,
            )
        ],
        [
            # Unsignificant impact
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                ["sequence_variant"],
                None,
                None,
                None,
                None,
                None,
            )
        ],
        [
            # Unknown impact
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                ["potential_new_term"],
                None,
                None,
                None,
                None,
                None,
            )
        ],
    ]
    expected_output = [
        [
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                "SO_0001587",
                "stop_gained",
                None,
                1.0,
                "vep",
                "variantConsequence",
            )
        ],
        [
            (
                "1_248441764_C_T",
                "1",
                "ENSG00000227152",
                "SO_0001630",
                "splice_region_variant",
                None,
                0.33,
                "vep",
                "variantConsequence",
            )
        ],
        [],
        [],
    ]

    @pytest.fixture(scope="class")
    def mock_variant_consequence_df(
        self: TestGetVariantConsequences, spark: SparkSession
    ) -> DataFrame:
        """Mock Dataframe of the LUT that contains all possible functional consequences of a variant."""
        return spark.createDataFrame(
            data=[
                # High impact term
                ("SO_0001587", "stop_gained", 1.0),
                # Moderate impact term
                ("SO_0001583", "missense_variant", 0.66),
                # Low impact term
                (
                    "SO_0001630",
                    "splice_region_variant",
                    0.33,
                ),
                # Modifier impact term
                ("SO_0001623", "5_prime_UTR_variant", 0.1),
                # Unsignificant impact term
                ("SO_0001060", "sequence_variant", None),
            ],
            schema=["variantFunctionalConsequenceId", "label", "score"],
        )

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_coverage_of_consequence_evidence(
        self: TestGetVariantConsequences,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        mock_variant_consequence_df: DataFrame,
        variant_df_schema: t.StructType,
        v2g_df_schema: t.StructType,
    ) -> None:
        """Test the get_variant_consequences util."""
        mock_df = spark.createDataFrame(
            data=test_input, schema=variant_df_schema
        ).select(
            "variantId",
            "chromosome",
            f.struct("gene_id", "consequence_terms").alias("transcriptConsequence"),
        )
        test_df = (
            mock_df.transform(
                lambda df: get_variant_consequences(df, mock_variant_consequence_df)
            )
            .toPandas()
            .dropna(axis=1, how="all")
        )
        expected_df = (
            spark.createDataFrame(data=expected_output, schema=v2g_df_schema)
            .toPandas()
            .dropna(axis=1, how="all")
        )
        print(test_df.head())
        print(expected_df.head())
        assert_frame_equal(
            test_df,
            expected_df,
            check_like=True,
        )


class TestGetPolypyhenScoreAndGetSiftScore:
    """Test the get_polyphen_scores util."""

    test_input = [
        [
            (
                # Variant with score info
                "19_900951_A_G",
                "19",
                "ENSG00000198858",
                None,
                0.355,
                "benign",
                0.91,
                "tolerated",
                None,
            )
        ],
        [
            (
                # No score info
                "19_900951_A_G",
                "19",
                "ENSG00000198858",
                None,
                None,
                None,
                None,
                None,
                None,
            )
        ],
    ]
    expected_output = [
        [
            # One V2G evidence per score
            (
                "19_900951_A_G",
                "19",
                "ENSG00000198858",
                None,
                "benign",
                None,
                0.355,
                "vep",
                "polyphen",
            ),
            (
                "19_900951_A_G",
                "19",
                "ENSG00000198858",
                None,
                "tolerated",
                None,
                0.09,
                "vep",
                "sift",
            ),
        ],
        [],  # Empty dataframe
    ]

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_coverage_of_score_evidence(
        self: TestGetPolypyhenScoreAndGetSiftScore,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        variant_df_schema: t.StructType,
        v2g_df_schema: t.StructType,
    ) -> None:
        """Tests that polyphen and sift scores are correctly retrieved."""
        mock_df = spark.createDataFrame(
            data=test_input, schema=variant_df_schema
        ).select(
            "variantId",
            "chromosome",
            f.struct(
                "gene_id",
                "polyphen_score",
                "polyphen_prediction",
                "sift_score",
                "sift_prediction",
            ).alias("transcriptConsequence"),
        )
        test_df = (
            mock_df.transform(get_polyphen_score)
            .unionByName(mock_df.transform(get_sift_score))
            .toPandas()
            .dropna(axis=1, how="all")
        )
        expected_df = (
            spark.createDataFrame(data=expected_output, schema=v2g_df_schema)
            .toPandas()
            .dropna(axis=1, how="all")
        )

        assert_frame_equal(test_df, expected_df, check_like=True)


class TestGetPlofFlag:
    """Test the get_plof_flag util."""

    test_input = [
        [
            (
                # High confidence pLOF variant
                "1_48242556_G_T",
                "1",
                "ENSG00000117834",
                None,
                None,
                None,
                None,
                None,
                "HC",
            )
        ],
        [
            (
                # Low confidence pLOF variant
                "1_46615007_G_A",
                "1",
                "ENSG00000142961",
                None,
                None,
                None,
                None,
                None,
                "LC",
            )
        ],
        [
            (
                # No pLOF flag
                "1_48242556_G_T",
                "1",
                "ENSG00000117834",
                None,
                None,
                None,
                None,
                None,
                None,
            )
        ],
    ]

    expected_output = [
        [
            (
                "1_48242556_G_T",
                "1",
                "ENSG00000117834",
                None,
                None,
                True,
                1.0,
                "vep",
                "loftee",
            )
        ],
        [
            (
                "1_46615007_G_A",
                "1",
                "ENSG00000142961",
                None,
                None,
                False,
                0.0,
                "vep",
                "loftee",
            )
        ],
        [],
    ]

    @pytest.mark.parametrize(
        "test_input, expected_output", zip(test_input, expected_output)
    )
    def test_coverage_of_plof_evidence(
        self: TestGetPlofFlag,
        spark: SparkSession,
        test_input: list[tuple],
        expected_output: list[tuple],
        variant_df_schema: t.StructType,
        v2g_df_schema: t.StructType,
    ) -> None:
        """Tests that plof flag is correctly retrieved."""
        mock_df = spark.createDataFrame(
            data=test_input, schema=variant_df_schema
        ).select(
            "variantId",
            "chromosome",
            f.struct("gene_id", "lof").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(get_plof_flag).toPandas().dropna(axis=1, how="all")
        expected_df = (
            spark.createDataFrame(data=expected_output, schema=v2g_df_schema)
            .toPandas()
            .dropna(axis=1, how="all")
        )

        assert_frame_equal(
            test_df,
            expected_df,
            check_like=True,
            check_dtype=False,
        )
