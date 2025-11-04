"""Test colocalisation methods.

This test module uses PySpark DataFrames and Rows heavily. To keep Pylance happy
without changing runtime behavior, we add a few targeted casts and avoid
string-based Row indexing in favor of ``asDict()`` accessors.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.method.colocalisation import Coloc, ColocPIP, ECaviar


def test_coloc(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test coloc."""
    assert isinstance(Coloc.colocalise(mock_study_locus_overlap), Colocalisation)
    assert isinstance(
        Coloc.colocalise(
            mock_study_locus_overlap, priorc1=1e-4, priorc2=1e-4, priorc12=1e-5
        ),
        Colocalisation,
    )


@pytest.mark.parametrize(
    "observed_data, expected_data",
    [
        # associations with a single overlapping SNP
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp",
                    "statistics": {
                        "left_logBF": 10.3,
                        "right_logBF": 10.5,
                        "left_beta": 0.1,
                        "right_beta": 0.2,
                        "left_posteriorProbability": 0.91,
                        "right_posteriorProbability": 0.92,
                    },
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
        # Case with mismatched posterior probabilities:
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": 10.5,
                        "left_beta": 0.001,
                        "right_beta": 0.2,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.92,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_logBF": 10.3,
                        "right_logBF": 3.8,
                        "left_beta": 0.3,
                        "right_beta": 0.005,
                        "left_posteriorProbability": 0.91,
                        "right_posteriorProbability": 0.01,
                    },
                },
            ],
            # expected coloc
            [],
        ),
        # Case of an overlap with significant PP overlap:
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_logBF": 10.2,
                        "right_logBF": 10.5,
                        "left_beta": 0.5,
                        "right_beta": 0.2,
                        "left_posteriorProbability": 0.91,
                        "right_posteriorProbability": 0.92,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": 3.8,
                        "left_beta": 0.003,
                        "right_beta": 0.005,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.01,
                    },
                },
            ],
            # expected coloc
            [
                {
                    "h0": 1.02277006860577e-4,
                    "h1": 2.7519169183135977e-4,
                    "h2": 3.718812819512325e-4,
                    "h3": 1.3533048074295033e-6,
                    "h4": 0.9992492967145488,
                },
            ],
        ),
        # Case where the overlap source is ["left", "both", "both"]:
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": None,
                        "left_beta": 0.003,
                        "right_beta": None,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.01,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": 3.8,
                        "left_beta": 0.003,
                        "right_beta": 0.005,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.01,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp3",
                    "statistics": {
                        "left_logBF": 10.2,
                        "right_logBF": 10.5,
                        "left_beta": 0.5,
                        "right_beta": 0.2,
                        "left_posteriorProbability": 0.91,
                        "right_posteriorProbability": 0.92,
                    },
                },
            ],
            # expected coloc
            [
                {
                    "h0": 1.02277006860577e-4,
                    "h1": 2.752255943423052e-4,
                    "h2": 3.718914358059273e-4,
                    "h3": 1.5042926116520848e-6,
                    "h4": 0.9992491016906891,
                },
            ],
        ),
        # Case where PPs are high on the left, but low on the right:
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": None,
                        "left_beta": 0.003,
                        "right_beta": None,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.01,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_logBF": 1.2,
                        "right_logBF": 3.8,
                        "left_beta": 0.003,
                        "right_beta": 0.005,
                        "left_posteriorProbability": 0.001,
                        "right_posteriorProbability": 0.01,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp3",
                    "statistics": {
                        "left_logBF": 10.2,
                        "right_logBF": 10.5,
                        "left_beta": 0.5,
                        "right_beta": 0.2,
                        "left_posteriorProbability": 0.09,
                        "right_posteriorProbability": 0.92,
                    },
                },
            ],
            # expected coloc
            [],
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
        _df=spark.createDataFrame(
            cast(Any, observed_data), schema=StudyLocusOverlap.get_schema()
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    observed_coloc_df = cast(
        DataFrame,
        Coloc.colocalise(
            observed_overlap, overlap_size_cutoff=5, posterior_cutoff=0.1
        ).df,
    )

    # Define schema for the expected DataFrame
    result_schema = StructType(
        [
            StructField("h0", DoubleType(), True),
            StructField("h1", DoubleType(), True),
            StructField("h2", DoubleType(), True),
            StructField("h3", DoubleType(), True),
            StructField("h4", DoubleType(), True),
        ]
    )

    if not expected_data:
        expected_coloc_df = spark.createDataFrame([], schema=result_schema)
    else:
        expected_coloc_df = spark.createDataFrame(expected_data, schema=result_schema)

    # Pylance can struggle to infer the type of ``rdd`` on a Spark DataFrame.
    # A light cast to ``Any`` avoids false positives without affecting runtime.
    if cast(Any, observed_coloc_df).rdd.isEmpty():
        observed_coloc_df = spark.createDataFrame([], schema=result_schema)

    observed_coloc_df = observed_coloc_df.select("h0", "h1", "h2", "h3", "h4")

    observed_coloc_pdf = observed_coloc_df.toPandas()
    expected_coloc_pdf = expected_coloc_df.toPandas()

    if expected_coloc_pdf.empty:
        assert observed_coloc_pdf.empty, (
            f"Expected an empty DataFrame, but got:\n{observed_coloc_pdf}"
        )
    else:
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
                cast(
                    Any,
                    [
                        {
                            "leftStudyLocusId": "1",
                            "rightStudyLocusId": "2",
                            "rightStudyType": "eqtl",
                            "chromosome": "1",
                            "tagVariantId": "snp",
                            "statistics": {
                                "left_logBF": None,
                                "right_logBF": None,
                                "left_beta": 0.1,
                                "right_beta": 0.2,
                                "left_posteriorProbability": 0.91,
                                "right_posteriorProbability": 0.92,
                            },  # irrelevant for COLOC
                        }
                    ],
                ),
                schema=StructType(
                    [
                        StructField("leftStudyLocusId", StringType(), False),
                        StructField("rightStudyLocusId", StringType(), False),
                        StructField("rightStudyType", StringType(), False),
                        StructField("chromosome", StringType(), False),
                        StructField("tagVariantId", StringType(), False),
                        StructField(
                            "statistics",
                            StructType(
                                [
                                    StructField("left_logBF", DoubleType(), True),
                                    StructField("right_logBF", DoubleType(), True),
                                    StructField("left_beta", DoubleType(), False),
                                    StructField("right_beta", DoubleType(), False),
                                    StructField(
                                        "left_posteriorProbability", DoubleType(), True
                                    ),
                                    StructField(
                                        "right_posteriorProbability", DoubleType(), True
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            )
        ),
        StudyLocusOverlap.get_schema(),
    )
    observed_coloc_df = cast(DataFrame, Coloc.colocalise(observed_overlap).df)
    row_h0 = observed_coloc_df.select("h0").collect()[0].asDict()
    assert row_h0["h0"] > minimum_expected_h0, (
        "COLOC should return a high h0 (no association) when the input data has irrelevant logBF."
    )
    row_h4 = observed_coloc_df.select("h4").collect()[0].asDict()
    assert row_h4["h4"] < maximum_expected_h4, (
        "COLOC should return a low h4 (traits are associated) when the input data has irrelevant logBF."
    )


def test_coloc_no_betas(spark: SparkSession) -> None:
    """Test COLOC output when the input data has no betas."""
    observed_overlap = StudyLocusOverlap(
        (
            spark.createDataFrame(
                cast(
                    Any,
                    [
                        {
                            "leftStudyLocusId": "1",
                            "rightStudyLocusId": "2",
                            "rightStudyType": "eqtl",
                            "chromosome": "1",
                            "tagVariantId": "snp",
                            "statistics": {
                                "left_logBF": 10.5,
                                "right_logBF": 10.3,
                                "left_beta": None,
                                "right_beta": None,
                                "left_posteriorProbability": 0.91,
                                "right_posteriorProbability": 0.92,
                            },  # irrelevant for COLOC
                        }
                    ],
                ),
                schema=StructType(
                    [
                        StructField("leftStudyLocusId", StringType(), False),
                        StructField("rightStudyLocusId", StringType(), False),
                        StructField("rightStudyType", StringType(), False),
                        StructField("chromosome", StringType(), False),
                        StructField("tagVariantId", StringType(), False),
                        StructField(
                            "statistics",
                            StructType(
                                [
                                    StructField("left_logBF", DoubleType(), False),
                                    StructField("right_logBF", DoubleType(), False),
                                    StructField("left_beta", DoubleType(), True),
                                    StructField("right_beta", DoubleType(), True),
                                    StructField(
                                        "left_posteriorProbability", DoubleType(), True
                                    ),
                                    StructField(
                                        "right_posteriorProbability", DoubleType(), True
                                    ),
                                ]
                            ),
                        ),
                    ]
                ),
            )
        ),
        StudyLocusOverlap.get_schema(),
    )
    observed_coloc_df = cast(DataFrame, Coloc.colocalise(observed_overlap).df)
    beta_ratio_row = (
        observed_coloc_df.select("betaRatioSignAverage").collect()[0].asDict()
    )
    assert beta_ratio_row["betaRatioSignAverage"] is None, (
        "No betas results in None type."
    )


def test_ecaviar(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test eCAVIAR."""
    assert isinstance(ECaviar.colocalise(mock_study_locus_overlap), Colocalisation)


def test_coloc_pip(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test ColocPIP basic functionality."""
    assert isinstance(ColocPIP.colocalise(mock_study_locus_overlap), Colocalisation)
    assert isinstance(
        ColocPIP.colocalise(
            mock_study_locus_overlap, priorc1=1e-4, priorc2=1e-4, priorc12=1e-5
        ),
        Colocalisation,
    )


@pytest.mark.parametrize(
    "observed_data, expected_data",
    [
        # Case with a single overlapping SNP with high PIPs
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_posteriorProbability": 0.95,
                        "right_posteriorProbability": 0.90,
                        "left_beta": 0.5,
                        "right_beta": 0.3,
                    },
                },
            ],
            # expected coloc - high H4 expected for shared causal
            {
                "h4_greater_than": 0.9,
            },
        ),
        # Case with multiple overlapping SNPs with high PIPs
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_posteriorProbability": 0.8,
                        "right_posteriorProbability": 0.85,
                        "left_beta": 0.5,
                        "right_beta": 0.3,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_posteriorProbability": 0.15,
                        "right_posteriorProbability": 0.10,
                        "left_beta": 0.1,
                        "right_beta": 0.05,
                    },
                },
            ],
            # expected coloc - high H4 from overlapping PIPs
            {
                "h4_greater_than": 0.9,
            },
        ),
        # Case with high PIP in left but low in right - still shows shared signal in ColocPIP
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_posteriorProbability": 0.9,
                        "right_posteriorProbability": 0.05,
                        "left_beta": 0.5,
                        "right_beta": 0.1,
                    },
                },
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp2",
                    "statistics": {
                        "left_posteriorProbability": 0.05,
                        "right_posteriorProbability": 0.9,
                        "left_beta": 0.1,
                        "right_beta": 0.5,
                    },
                },
            ],
            # expected coloc - ColocPIP still finds high H4 due to overlap
            {
                "h3_greater_than": 0.001,
                "h4_greater_than": 0.5,
            },
        ),
        # Case with moderate PIPs showing both signals
        (
            # observed overlap
            [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "2",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "snp1",
                    "statistics": {
                        "left_posteriorProbability": 0.5,
                        "right_posteriorProbability": 0.5,
                        "left_beta": 0.3,
                        "right_beta": 0.3,
                    },
                },
            ],
            # expected coloc - moderate H4
            {
                "h4_greater_than": 0.5,
            },
        ),
    ],
)
def test_coloc_pip_semantic(
    spark: SparkSession,
    observed_data: list[Any],
    expected_data: dict[str, float],
) -> None:
    """Test ColocPIP with various PIP patterns."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(Any, observed_data), schema=StudyLocusOverlap.get_schema()
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    observed_coloc_df = cast(DataFrame, ColocPIP.colocalise(observed_overlap).df)

    assert not observed_coloc_df.rdd.isEmpty(), (
        "ColocPIP should return results for valid overlaps"
    )

    # Collect the results
    results = observed_coloc_df.collect()[0]
    results_dict = results.asDict()

    # Check H3 (distinct causal)
    if "h3_greater_than" in expected_data:
        assert results_dict["h3"] >= expected_data["h3_greater_than"], (
            f"Expected h3 >= {expected_data['h3_greater_than']}, got {results_dict['h3']}"
        )
    if "h3_less_than" in expected_data:
        assert results_dict["h3"] < expected_data["h3_less_than"], (
            f"Expected h3 < {expected_data['h3_less_than']}, got {results_dict['h3']}"
        )

    # Check H4 (shared causal)
    if "h4_greater_than" in expected_data:
        assert results_dict["h4"] >= expected_data["h4_greater_than"], (
            f"Expected h4 >= {expected_data['h4_greater_than']}, got {results_dict['h4']}"
        )
    if "h4_less_than" in expected_data:
        assert results_dict["h4"] < expected_data["h4_less_than"], (
            f"Expected h4 < {expected_data['h4_less_than']}, got {results_dict['h4']}"
        )

    # Check that H0, H1, H2 are zero (ColocPIP only calculates H3 and H4)
    assert results_dict["h0"] == 0.0, "H0 should be 0 in ColocPIP"
    assert results_dict["h1"] == 0.0, "H1 should be 0 in ColocPIP"
    assert results_dict["h2"] == 0.0, "H2 should be 0 in ColocPIP"

    # Check that H3 + H4 approximately equals 1 (they are normalized)
    assert abs((results_dict["h3"] + results_dict["h4"]) - 1.0) < 0.01, (
        f"H3 + H4 should approximately equal 1, got {results_dict['h3'] + results_dict['h4']}"
    )


def test_coloc_pip_priors(spark: SparkSession) -> None:
    """Test that ColocPIP correctly responds to custom priors."""
    # Multiple SNPs with moderate PIPs where priors will have an effect
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(
                Any,
                [
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp1",
                        "statistics": {
                            "left_posteriorProbability": 0.4,
                            "right_posteriorProbability": 0.35,
                            "left_beta": 0.3,
                            "right_beta": 0.25,
                        },
                    },
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp2",
                        "statistics": {
                            "left_posteriorProbability": 0.3,
                            "right_posteriorProbability": 0.4,
                            "left_beta": 0.2,
                            "right_beta": 0.3,
                        },
                    },
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp3",
                        "statistics": {
                            "left_posteriorProbability": 0.2,
                            "right_posteriorProbability": 0.15,
                            "left_beta": 0.15,
                            "right_beta": 0.1,
                        },
                    },
                ],
            ),
            schema=StudyLocusOverlap.get_schema(),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    # Test with higher p12 prior (should favor H4 - shared causal)
    result_high_p12 = (
        ColocPIP.colocalise(observed_overlap, priorc1=1e-4, priorc2=1e-4, priorc12=1e-3)
        .df.collect()[0]
        .asDict()
    )

    # Test with lower p12 prior (should favor H3 - distinct causal)
    result_low_p12 = (
        ColocPIP.colocalise(observed_overlap, priorc1=1e-4, priorc2=1e-4, priorc12=1e-6)
        .df.collect()[0]
        .asDict()
    )

    # Higher p12 should lead to higher H4 (shared causal)
    assert result_high_p12["h4"] > result_low_p12["h4"], (
        f"Higher p12 prior should increase H4. "
        f"Got high_p12 H4={result_high_p12['h4']:.4f}, low_p12 H4={result_low_p12['h4']:.4f}"
    )

    # Lower p12 should lead to higher H3 (distinct causal)
    assert result_low_p12["h3"] > result_high_p12["h3"], (
        f"Lower p12 prior should increase H3. "
        f"Got high_p12 H3={result_high_p12['h3']:.4f}, low_p12 H3={result_low_p12['h3']:.4f}"
    )

    # Verify H3 + H4 equals 1 (they are normalized)
    assert abs((result_high_p12["h3"] + result_high_p12["h4"]) - 1.0) < 0.01
    assert abs((result_low_p12["h3"] + result_low_p12["h4"]) - 1.0) < 0.01


def test_coloc_pip_type_error(mock_study_locus_overlap: StudyLocusOverlap) -> None:
    """Test that ColocPIP raises TypeError for incorrect prior types."""
    with pytest.raises(TypeError):
        ColocPIP.colocalise(
            mock_study_locus_overlap,
            priorc1="invalid",
            priorc2=1e-4,
            priorc12=1e-5,
        )


def test_coloc_pip_null_pips(spark: SparkSession) -> None:
    """Test ColocPIP handles null PIPs by filling with zeros."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(
                Any,
                [
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp1",
                        "statistics": {
                            "left_posteriorProbability": None,
                            "right_posteriorProbability": 0.9,
                            "left_beta": 0.5,
                            "right_beta": 0.3,
                        },
                    },
                ],
            ),
            schema=StructType(
                [
                    StructField("leftStudyLocusId", StringType(), False),
                    StructField("rightStudyLocusId", StringType(), False),
                    StructField("rightStudyType", StringType(), False),
                    StructField("chromosome", StringType(), False),
                    StructField("tagVariantId", StringType(), False),
                    StructField(
                        "statistics",
                        StructType(
                            [
                                StructField(
                                    "left_posteriorProbability", DoubleType(), True
                                ),
                                StructField(
                                    "right_posteriorProbability", DoubleType(), True
                                ),
                                StructField("left_beta", DoubleType(), True),
                                StructField("right_beta", DoubleType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    # Should not raise an error
    result = ColocPIP.colocalise(observed_overlap).df
    assert not result.rdd.isEmpty(), "Should handle null PIPs gracefully"


def test_coloc_pip_beta_ratio(spark: SparkSession) -> None:
    """Test that ColocPIP includes beta ratio calculations."""
    observed_overlap = StudyLocusOverlap(
        _df=spark.createDataFrame(
            cast(
                Any,
                [
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "rightStudyType": "eqtl",
                        "chromosome": "1",
                        "tagVariantId": "snp1",
                        "statistics": {
                            "left_posteriorProbability": 0.9,
                            "right_posteriorProbability": 0.85,
                            "left_beta": 0.5,
                            "right_beta": 0.3,
                        },
                    },
                ],
            ),
            schema=StudyLocusOverlap.get_schema(),
        ),
        _schema=StudyLocusOverlap.get_schema(),
    )

    result = ColocPIP.colocalise(observed_overlap).df.collect()[0]

    # Check that beta ratio fields exist
    assert "betaRatioSignAverage" in result.asDict(), (
        "Beta ratio should be included in results"
    )
