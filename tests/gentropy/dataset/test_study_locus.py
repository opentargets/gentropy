"""Test study locus dataset."""

from __future__ import annotations

from typing import Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest
from pyspark.sql import Column, Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.ld_index import LDIndex
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import (
    CredibleInterval,
    StudyLocus,
    StudyLocusQualityCheck,
)
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_index import VariantIndex


@pytest.mark.parametrize(
    "has_overlap, expected",
    [
        # Overlap exists
        (
            True,
            [
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "commonTag",
                    "statistics": {
                        "left_posteriorProbability": 0.9,
                        "right_posteriorProbability": 0.6,
                    },
                },
                {
                    "leftStudyLocusId": 1,
                    "rightStudyLocusId": 2,
                    "chromosome": "1",
                    "tagVariantId": "nonCommonTag",
                    "statistics": {
                        "left_posteriorProbability": None,
                        "right_posteriorProbability": 0.6,
                    },
                },
            ],
        ),
        # No overlap
        (False, []),
    ],
)
def test_find_overlaps_semantic(
    spark: SparkSession, has_overlap: bool, expected: list[Any]
) -> None:
    """Test study locus overlaps with and without actual overlap."""
    if has_overlap:
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with a common variant in the locus
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.6},
                            {"variantId": "nonCommonTag", "posteriorProbability": 0.6},
                        ],
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )
    else:
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with no common variants in the locus
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {"variantId": "var1", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",
                        "locus": None,
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )

    studies = StudyIndex(
        _df=spark.createDataFrame(
            [
                {
                    "studyId": "study1",
                    "studyType": "gwas",
                    "traitFromSource": "trait1",
                    "projectId": "project1",
                },
                {
                    "studyId": "study2",
                    "studyType": "eqtl",
                    "traitFromSource": "trait2",
                    "projectId": "project2",
                },
            ]
        ),
        _schema=StudyIndex.get_schema(),
    )
    expected_overlaps_df = spark.createDataFrame(
        expected, StudyLocusOverlap.get_schema()
    )
    cols_to_compare = [
        "tagVariantId",
        "statistics.left_posteriorProbability",
        "statistics.right_posteriorProbability",
    ]
    assert (
        credset.find_overlaps(studies).df.select(*cols_to_compare).collect()
        == expected_overlaps_df.select(*cols_to_compare).collect()
    ), "Overlaps differ from expected."


def test_find_overlaps(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex
) -> None:
    """Test study locus overlaps."""
    assert isinstance(
        mock_study_locus.find_overlaps(mock_study_index), StudyLocusOverlap
    )


@pytest.mark.parametrize(
    "study_type, expected_sl_count", [("gwas", 1), ("eqtl", 1), ("pqtl", 0)]
)
def test_filter_by_study_type(
    spark: SparkSession, study_type: str, expected_sl_count: int
) -> None:
    """Test filter by study type."""
    # Input data
    sl = StudyLocus(
        _df=spark.createDataFrame(
            [
                {
                    # from gwas
                    "studyLocusId": 1,
                    "variantId": "lead1",
                    "studyId": "study1",
                },
                {
                    # from eqtl
                    "studyLocusId": 2,
                    "variantId": "lead2",
                    "studyId": "study2",
                },
            ],
            StudyLocus.get_schema(),
        ),
        _schema=StudyLocus.get_schema(),
    )
    studies = StudyIndex(
        _df=spark.createDataFrame(
            [
                {
                    "studyId": "study1",
                    "studyType": "gwas",
                    "traitFromSource": "trait1",
                    "projectId": "project1",
                },
                {
                    "studyId": "study2",
                    "studyType": "eqtl",
                    "traitFromSource": "trait2",
                    "projectId": "project2",
                },
            ]
        ),
        _schema=StudyIndex.get_schema(),
    )

    observed = sl.filter_by_study_type(study_type, studies)
    assert observed.df.count() == expected_sl_count


def test_annotate_locus_statistics(
    mock_study_locus: StudyLocus, mock_summary_statistics: SummaryStatistics
) -> None:
    """Test annotate locus statistics returns a StudyLocus."""
    assert isinstance(
        mock_study_locus.annotate_locus_statistics(mock_summary_statistics, 100),
        StudyLocus,
    )


def test_filter_credible_set(mock_study_locus: StudyLocus) -> None:
    """Test credible interval filter."""
    assert isinstance(
        mock_study_locus.filter_credible_set(CredibleInterval.IS95), StudyLocus
    )


def test_assign_study_locus_id__null_variant_id(spark: SparkSession) -> None:
    """Test assign study locus id when variant id is null for the same study."""
    df = spark.createDataFrame(
        [("GCST000001", None), ("GCST000001", None)],
        schema="studyId: string, variantId: string",
    ).withColumn(
        "studyLocusId",
        StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
    )
    assert (
        df.select("studyLocusId").distinct().count() == 2
    ), "studyLocusId is not unique when variantId is null"


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Locus is not null, should return union between variants in locus and lead variant
            [
                (
                    1,
                    "traitA",
                    "22_varA",
                    [
                        {"variantId": "22_varA", "posteriorProbability": 0.44},
                        {"variantId": "22_varB", "posteriorProbability": 0.015},
                    ],
                ),
            ],
            [
                (
                    "22_varA",
                    "22",
                ),
                (
                    "22_varB",
                    "22",
                ),
            ],
        ),
        (
            # locus is null, should return lead variant
            [
                (1, "traitA", "22_varA", None),
            ],
            [
                (
                    "22_varA",
                    "22",
                ),
            ],
        ),
    ],
)
def test_unique_variants_in_locus(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test unique variants in locus."""
    # assert isinstance(mock_study_locus.test_unique_variants_in_locus(), DataFrame)
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema), _schema=StudyLocus.get_schema()
    )
    expected_df = spark.createDataFrame(
        expected, schema="variantId: string, chromosome: string"
    )
    assert data_sl.unique_variants_in_locus().collect() == expected_df.collect()


def test_neglog_pvalue(mock_study_locus: StudyLocus) -> None:
    """Test neglog pvalue."""
    assert isinstance(mock_study_locus.neglog_pvalue(), Column)


def test_clump(mock_study_locus: StudyLocus) -> None:
    """Test clump."""
    assert isinstance(mock_study_locus.clump(), StudyLocus)


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # Simple case
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [{"variantId": "tagVariantA", "posteriorProbability": 1.0}],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    [
                        {
                            "variantId": "tagVariantA",
                            "posteriorProbability": 1.0,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        }
                    ],
                )
            ],
        ),
        (
            # Unordered credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {"variantId": "tagVariantA", "posteriorProbability": 0.44},
                        {"variantId": "tagVariantB", "posteriorProbability": 0.015},
                        {"variantId": "tagVariantC", "posteriorProbability": 0.04},
                        {"variantId": "tagVariantD", "posteriorProbability": 0.005},
                        {"variantId": "tagVariantE", "posteriorProbability": 0.5},
                        {"variantId": "tagVariantNull", "posteriorProbability": None},
                        {"variantId": "tagVariantNull", "posteriorProbability": None},
                    ],
                )
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadA",
                    [
                        {
                            "variantId": "tagVariantE",
                            "posteriorProbability": 0.5,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantA",
                            "posteriorProbability": 0.44,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantC",
                            "posteriorProbability": 0.04,
                            "is95CredibleSet": True,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantB",
                            "posteriorProbability": 0.015,
                            "is95CredibleSet": False,
                            "is99CredibleSet": True,
                        },
                        {
                            "variantId": "tagVariantD",
                            "posteriorProbability": 0.005,
                            "is95CredibleSet": False,
                            "is99CredibleSet": False,
                        },
                        {
                            "variantId": "tagVariantNull",
                            "posteriorProbability": None,
                            "is95CredibleSet": False,
                            "is99CredibleSet": False,
                        },
                        {
                            "variantId": "tagVariantNull",
                            "posteriorProbability": None,
                            "is95CredibleSet": False,
                            "is99CredibleSet": False,
                        },
                    ],
                )
            ],
        ),
        (
            # Null credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                )
            ],
        ),
        (
            # Empty credible set
            [
                # Observed
                (
                    1,
                    "traitA",
                    "leadB",
                    [],
                ),
            ],
            [
                # Expected
                (
                    1,
                    "traitA",
                    "leadB",
                    None,
                )
            ],
        ),
    ],
)
def test_annotate_credible_sets(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test annotate_credible_sets."""
    schema = StructType(
        [
            StructField("studyLocusId", LongType(), True),
            StructField("studyId", StringType(), True),
            StructField("variantId", StringType(), True),
            StructField(
                "locus",
                ArrayType(
                    StructType(
                        [
                            StructField("variantId", StringType(), True),
                            StructField("posteriorProbability", DoubleType(), True),
                            StructField("is95CredibleSet", BooleanType(), True),
                            StructField("is99CredibleSet", BooleanType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    data_sl = StudyLocus(
        _df=spark.createDataFrame(observed, schema), _schema=StudyLocus.get_schema()
    )
    expected_sl = StudyLocus(
        _df=spark.createDataFrame(expected, schema), _schema=StudyLocus.get_schema()
    )
    assert data_sl.annotate_credible_sets().df.collect() == expected_sl.df.collect()


def test_annotate_ld(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex, mock_ld_index: LDIndex
) -> None:
    """Test annotate_ld."""
    assert isinstance(
        mock_study_locus.annotate_ld(mock_study_index, mock_ld_index), StudyLocus
    )


def test__qc_no_population(mock_study_locus: StudyLocus) -> None:
    """Test _qc_no_population."""
    assert isinstance(mock_study_locus._qc_no_population(), StudyLocus)


def test_qc_MHC_region(mock_study_locus: StudyLocus) -> None:
    """Test qc_MHC_region."""
    assert isinstance(mock_study_locus.qc_MHC_region(), StudyLocus)


def test_ldannotate(
    mock_study_locus: StudyLocus, mock_study_index: StudyIndex, mock_ld_index: LDIndex
) -> None:
    """Test ldannotate."""
    assert isinstance(
        mock_study_locus.annotate_ld(mock_study_index, mock_ld_index), StudyLocus
    )


def test_filter_ld_set(spark: SparkSession) -> None:
    """Test filter_ld_set."""
    observed_data = [
        Row(studyLocusId="sl1", ldSet=[{"tagVariantId": "tag1", "r2Overall": 0.4}])
    ]
    observed_df = spark.createDataFrame(
        observed_data, ["studyLocusId", "ldSet"]
    ).withColumn("ldSet", StudyLocus.filter_ld_set(f.col("ldSet"), 0.5))
    expected_tags_in_ld = 0
    assert (
        observed_df.filter(f.size("ldSet") > 1).count() == expected_tags_in_ld
    ), "Expected tags in ld set differ from observed."


def test_annotate_locus_statistics_boundaries(
    mock_study_locus: StudyLocus, mock_summary_statistics: SummaryStatistics
) -> None:
    """Test annotate locus statistics returns a StudyLocus."""
    df = mock_study_locus.df
    df = df.withColumn("locusStart", f.col("position") - 10)
    df = df.withColumn("locusEnd", f.col("position") + 10)
    slt = StudyLocus(df, StudyLocus.get_schema())
    assert isinstance(
        slt.annotate_locus_statistics_boundaries(mock_summary_statistics),
        StudyLocus,
    )


class TestStudyLocusVariantValidation:
    """Collection of tests for StudyLocus variant validation."""

    VARIANT_DATA = [
        ("v1", "c1", 1, "r", "a"),
        ("v2", "c1", 2, "r", "a"),
        ("v3", "c1", 3, "r", "a"),
        ("v4", "c1", 4, "r", "a"),
    ]
    VARIANT_HEADERS = [
        "variantId",
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
    ]

    STUDYLOCUS_DATA = [
        # First studylocus passes qc:
        (1, "v1", "s1", "v1"),
        (1, "v1", "s1", "v2"),
        (1, "v1", "s1", "v3"),
        # Second studylocus passes qc:
        (2, "v1", "s1", "v1"),
        (2, "v1", "s1", "v5"),
    ]
    STUDYLOCUS_HEADER = ["studyLocusId", "variantId", "studyId", "tagVariantId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusVariantValidation, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.variant_index = VariantIndex(
            _df=spark.createDataFrame(
                self.VARIANT_DATA, self.VARIANT_HEADERS
            ).withColumn("position", f.col("position").cast(t.IntegerType())),
            _schema=VariantIndex.get_schema(),
        )

        self.credible_set = StudyLocus(
            _df=(
                spark.createDataFrame(self.STUDYLOCUS_DATA, self.STUDYLOCUS_HEADER)
                .withColumn("studyLocusId", f.col("studyLocusId").cast(t.LongType()))
                .withColumn("qualityControls", f.array())
                .groupBy("studyLocusId", "variantId", "studyId")
                .agg(
                    f.collect_set(
                        f.struct(f.col("tagVariantId").alias("variantId"))
                    ).alias("locus")
                )
            ),
            _schema=StudyLocus.get_schema(),
        )

    def test_validation_return_type(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation returns the right type."""
        assert isinstance(
            self.credible_set.validate_variant_identifiers(self.variant_index),
            StudyLocus,
        )

    def test_validation_no_data_loss(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation returns same number of rows."""
        assert (
            self.credible_set.validate_variant_identifiers(
                self.variant_index
            ).df.count()
            == self.credible_set.df.count()
        )

    def test_validation_correctness(self: TestStudyLocusVariantValidation) -> None:
        """Testing if the validation flags the right number of variants."""
        # Execute validation:
        validated = self.credible_set.validate_variant_identifiers(
            self.variant_index
        ).df

        # Make sure there's only one study locus with a failed variants:
        assert validated.filter(f.size("qualityControls") > 0).count() == 1

        # Check that the right one is flagged:
        assert (
            validated.filter(
                (f.size("qualityControls") > 0) & (f.col("studyLocusId") == 2)
            ).count()
            == 1
        )


class TestStudyLocusValidation:
    """Collection of tests for StudyLocus validation."""

    STUDY_LOCUS_DATA = [
        # Won't be flagged:
        (1, "v1", "s1", 1.0, -8, []),
        # Already flagged, needs to be tested if the flag reamins unique:
        (2, "v2", "s2", 5.0, -4, [StudyLocusQualityCheck.SUBSIGNIFICANT_FLAG.value]),
        # To be flagged:
        (3, "v3", "s3", 1.0, -4, []),
        (4, "v4", "s4", 5.0, -3, []),
    ]

    STUDY_LOCUS_SCHEMA = t.StructType(
        [
            t.StructField("studyLocusId", t.LongType(), False),
            t.StructField("variantId", t.StringType(), False),
            t.StructField("studyId", t.StringType(), False),
            t.StructField("pValueMantissa", t.FloatType(), False),
            t.StructField("pValueExponent", t.IntegerType(), False),
            t.StructField("qualityControls", t.ArrayType(t.StringType()), False),
        ]
    )

    STUDY_DATA = [
        # Unflagged:
        ("s1", "p1", "gwas", []),
        # Flagged:
        ("s2", "p1", "gwas", ["some_flag"]),
        ("s3", "p1", "gwas", ["some_flag"]),
    ]

    STUDY_SCHEMA = t.StructType(
        [
            t.StructField("studyId", t.StringType(), False),
            t.StructField("projectId", t.StringType(), False),
            t.StructField("studyType", t.StringType(), False),
            t.StructField("qualityControls", t.ArrayType(t.StringType(), True), True),
        ]
    )

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyLocusValidation, spark: SparkSession) -> None:
        """Setup study locus for testing."""
        self.study_locus = StudyLocus(
            _df=spark.createDataFrame(
                self.STUDY_LOCUS_DATA, schema=self.STUDY_LOCUS_SCHEMA
            ),
            _schema=StudyLocus.get_schema(),
        )

        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, schema=self.STUDY_SCHEMA),
            _schema=StudyIndex.get_schema(),
        )

    @pytest.mark.parametrize(
        "test_pvalues",
        [1e-6, 1e-5],
    )
    def test_return_type_pval_validation(
        self: TestStudyLocusValidation, test_pvalues: float
    ) -> None:
        """Testing if the p-value validation returns the right type."""
        assert isinstance(
            self.study_locus.validate_lead_pvalue(test_pvalues), StudyLocus
        )

    @pytest.mark.parametrize(
        ("test_pvalues", "flagged_count"),
        [(1e-5, 3), (1e-4, 2)],
    )
    def test_flagged_count(
        self: TestStudyLocusValidation, test_pvalues: float, flagged_count: int
    ) -> None:
        """Testing if the p-value validation flags the right number of variants."""
        assert (
            self.study_locus.validate_lead_pvalue(test_pvalues)
            .df.filter(f.size("qualityControls") > 0)
            .count()
            == flagged_count
        )

    def test_flag_uniqueness(self: TestStudyLocusValidation) -> None:
        """Testing if the flag remains unique although the locus should be flagged twice."""
        assert (
            self.study_locus.validate_lead_pvalue(1e-10)
            .df.filter(f.size("qualityControls") > 1)
            .count()
        ) == 0

    def test_study_validation_return_type(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation returns the right type."""
        assert isinstance(self.study_locus.validate_study(self.study_index), StudyLocus)

    def test_study_validation_no_data_loss(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation returns same number of rows."""
        assert (
            self.study_locus.validate_study(self.study_index).df.count()
        ) == self.study_locus.df.count()

    def test_study_validation_correctness(self: TestStudyLocusValidation) -> None:
        """Testing if the study validation flags the right number of studies."""
        assert (
            self.study_locus.validate_study(self.study_index)
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"), StudyLocusQualityCheck.FAILED_STUDY.value
                )
            )
            .count()
        ) == 2

        assert (
            self.study_locus.validate_study(self.study_index)
            .df.filter(
                f.array_contains(
                    f.col("qualityControls"), StudyLocusQualityCheck.MISSING_STUDY.value
                )
            )
            .count()
        ) == 1
