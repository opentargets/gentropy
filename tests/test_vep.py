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
def mock_variant_consequence_df(spark: SparkSession) -> DataFrame:
    """Mock Dataframe of the LUT that contains all possible functional consequences of a variant."""
    return spark.createDataFrame(
        data=[
            # High impact term
            ["SO_0001587", "stop_gained", 1.0],
            # Moderate impact term
            ["SO_0001583", "missense_variant", 0.66],
            # Low impact term
            [
                "SO_0001630",
                "splice_region_variant",
                0.33,
            ],
            # Modifier impact term
            ["SO_0001623", "5_prime_UTR_variant", 0.1],
            # Unsignificant impact term
            ["SO_0001060", "sequence_variant", None],
        ],
        schema=["variantFunctionalConsequenceId", "label", "score"],
    )


@pytest.fixture(scope="module")
def vep_df_schema() -> t.StructType:
    """Schema of the mock VEP dataframe."""
    return t.StructType(
        [
            t.StructField("id", t.StringType(), False),
            t.StructField("gene_id", t.StringType(), False),
            t.StructField("consequence_terms", t.ArrayType(t.StringType()), True),
            t.StructField("polyphen_score", t.DoubleType(), True),
            t.StructField("polyphen_prediction", t.StringType(), True),
            t.StructField("sift_score", t.DoubleType(), True),
            t.StructField("sift_prediction", t.StringType(), True),
            t.StructField("lof", t.StringType(), True),
        ]
    )


class TestGetVariantConsequences:
    """Test the get_variant_consequences util."""

    def test_coverage_of_high_impact_variant(
        self: TestGetVariantConsequences, spark: SparkSession
    ) -> None:
        """Tests that a high impact variant is covered by the util."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    ["stop_gained"],
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "consequence_terms").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(
            lambda df: get_variant_consequences(df, mock_variant_consequence_df)
        )
        expected_df = spark.createDataFrame(
            [
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    "SO_0001587",
                    "stop_gained",
                    1.0,
                    "vep",
                    "variantConsequence",
                )
            ],
            [
                "variantId",
                "geneId",
                "variantFunctionalConsequenceId",
                "label",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(test_df.toPandas(), expected_df.toPandas(), check_like=True)

    def test_coverage_of_low_impact_variant(
        self: TestGetVariantConsequences, spark: SparkSession
    ) -> None:
        """Tests that a low impact variant is covered by the util. The functionality of getting the most severe one is also tested by comparing it with a term tagged as a modifier."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    ["splice_region_variant", "5_prime_UTR_variant"],
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "consequence_terms").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(
            lambda df: get_variant_consequences(df, mock_variant_consequence_df)
        )
        expected_df = spark.createDataFrame(
            [
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    "SO_0001630",
                    "splice_region_variant",
                    0.33,
                    "vep",
                    "variantConsequence",
                )
            ],
            [
                "variantId",
                "geneId",
                "variantFunctionalConsequenceId",
                "label",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(test_df.toPandas(), expected_df.toPandas(), check_like=True)

    def test_coverage_of_unsignificant_variant(
        self: TestGetVariantConsequences, spark: SparkSession
    ) -> None:
        """Tests that a non scored variant is not covered by the util."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    ["sequence_variant"],
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "consequence_terms").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(
            lambda df: get_variant_consequences(df, mock_variant_consequence_df)
        )
        assert test_df.count() == 0, "The dataframe should be empty."

    def test_unscored_consequence_term(
        self: TestGetVariantConsequences, spark: SparkSession
    ) -> None:
        """Tests that a consequence term absent from the LUT and without a score is not covered by the util."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_248441764_C_T",
                    "ENSG00000227152",
                    ["potential_new_term"],
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "consequence_terms").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(
            lambda df: get_variant_consequences(df, mock_variant_consequence_df)
        )
        assert test_df.count() == 0, "The dataframe should be empty."


class TestGetPolypyhenScoreAndGetSiftScore:
    """Test the get_polyphen_scores util."""

    def __init__(
        self: TestGetPolypyhenScoreAndGetSiftScore, spark: SparkSession
    ) -> None:
        """Initialize the class."""
        self.data_with_score_df = spark.createDataFrame(
            data=[
                (
                    "19_900951_A_G",
                    "ENSG00000198858",
                    None,
                    0.355,
                    "benign",
                    0.09,
                    "tolerated",
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct(
                "gene_id",
                "polyphen_score",
                "polyphen_prediction",
                "sift_score",
                "sift_prediction",
            ).alias("transcriptConsequence"),
        )
        self.data_without_score_df = spark.createDataFrame(
            data=[
                (
                    "19_900951_A_G",
                    "ENSG00000198858",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct(
                "gene_id",
                "polyphen_score",
                "polyphen_prediction",
                "sift_score",
                "sift_prediction",
            ).alias("transcriptConsequence"),
        )

    def test_coverage_of_variant_with_polyphen(
        self: TestGetPolypyhenScoreAndGetSiftScore, spark: SparkSession
    ) -> None:
        """Tests that polyphen scores are correctly retrieved."""
        mock_df = self.data_with_score_df
        test_df = mock_df.transform(get_polyphen_score)
        expected_df = spark.createDataFrame(
            [
                (
                    "19_900951_A_G",
                    "ENSG00000198858",
                    "benign",
                    0.355,
                    "vep",
                    "polyphen",
                )
            ],
            [
                "variantId",
                "geneId",
                "label",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(test_df.toPandas(), expected_df.toPandas(), check_like=True)

    def test_coverage_of_variant_with_sift(
        self: TestGetPolypyhenScoreAndGetSiftScore, spark: SparkSession
    ) -> None:
        """Tests that sift scores are correctly retrieved."""
        mock_df = self.data_with_score_df
        test_df = mock_df.transform(get_sift_score)
        expected_df = spark.createDataFrame(
            [
                (
                    "19_900951_A_G",
                    "ENSG00000198858",
                    "tolerated",
                    0.91,
                    "vep",
                    "sift",
                )
            ],
            [
                "variantId",
                "geneId",
                "label",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(test_df.toPandas(), expected_df.toPandas(), check_like=True)

    def test_coverage_of_variant_without_polyphen(
        self: TestGetPolypyhenScoreAndGetSiftScore,
    ) -> None:
        """Tests that a variant without polyphen score is not covered by the util."""
        mock_df = self.data_without_score_df
        test_df = mock_df.transform(get_polyphen_score)
        assert test_df.count() == 0, "The dataframe should be empty."

    def test_coverage_of_variant_without_sift(
        self: TestGetPolypyhenScoreAndGetSiftScore,
    ) -> None:
        """Tests that a variant without sift score is not covered by the util."""
        mock_df = self.data_without_score_df
        test_df = mock_df.transform(get_sift_score)
        assert test_df.count() == 0, "The dataframe should be empty."


class TestGetPlofFlag:
    """Test the get_plof_flag util."""

    def test_high_confidence_is_scored_with_1(
        self: TestGetPlofFlag, spark: SparkSession
    ) -> None:
        """Tests that variants predicted as a high confidence loss-of-function are scored with 1."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_48242556_G_T",
                    "ENSG00000117834",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "HC",
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "lof").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(get_plof_flag)
        expected_df = spark.createDataFrame(
            [
                (
                    "1_48242556_G_T",
                    "ENSG00000117834",
                    True,
                    1,
                    "vep",
                    "loftee",
                )
            ],
            [
                "variantId",
                "geneId",
                "isHighQualityPlof",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(
            test_df.toPandas(),
            expected_df.toPandas(),
            check_like=True,
            check_dtype=False,
        )

    def test_low_confidence_is_scored_with_0(
        self: TestGetPlofFlag, spark: SparkSession
    ) -> None:
        """Tests that variants predicted as a low confidence loss-of-function are scored with 0."""
        mock_df = spark.createDataFrame(
            data=[
                (
                    "1_46615007_G_A",
                    "ENSG00000142961",
                    None,
                    None,
                    None,
                    None,
                    None,
                    "LC",
                )
            ],
            schema=vep_df_schema(),
        ).select(
            "id",
            f.struct("gene_id", "lof").alias("transcriptConsequence"),
        )
        test_df = mock_df.transform(get_plof_flag)
        expected_df = spark.createDataFrame(
            [
                (
                    "1_46615007_G_A",
                    "ENSG00000142961",
                    False,
                    0,
                    "vep",
                    "loftee",
                )
            ],
            [
                "variantId",
                "geneId",
                "isHighQualityPlof",
                "score",
                "datatypeId",
                "datasourceId",
            ],
        )
        assert_frame_equal(
            test_df.toPandas(),
            expected_df.toPandas(),
            check_like=True,
            check_dtype=False,
        )
