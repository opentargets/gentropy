"""Test colocalisation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.types as t
import pytest
from pyspark.sql import functions as f

from otg.dataset.study_locus import StudyLocus
from otg.dataset.study_locus_overlap import StudyLocusOverlap, StudyLocusOverlapMethod

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from otg.dataset.study_index import StudyIndex


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


@pytest.mark.parametrize(
    "method", [StudyLocusOverlapMethod.LD, StudyLocusOverlapMethod.DISTANCE]
)
def test_study_locus_overlap_from_associations(
    mock_study_locus: StudyLocus,
    mock_study_index: StudyIndex,
    method: StudyLocusOverlapMethod,
) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(
        method,
        mock_study_locus,
        mock_study_index,
        distance_between_leads=500_000,
        distance_from_lead=500_000,
    )
    assert isinstance(overlaps, StudyLocusOverlap)


@pytest.mark.parametrize(
    ("observed", "expected"),
    [
        (
            # observed - input DataFrame representing gwas and nongwas data to find overlapping signals
            [
                {
                    "studyLocusId": 1,
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": 2,
                    "studyType": "eqtl",
                    "chromosome": "1",
                    "tagVariantId": "A",
                },
                {
                    "studyLocusId": 3,
                    "studyType": "gwas",
                    "chromosome": "1",
                    "tagVariantId": "B",
                },
            ],
            # expected - output DataFrame with overlapping signals
            [
                {"left_studyLocusId": 1, "right_studyLocusId": 2, "chromosome": "1"},
            ],
        ),
    ],
)
def test_overlapping_peaks(spark: SparkSession, observed: list, expected: list) -> None:
    """Test overlapping signals between GWAS-GWAS and GWAS-Molecular trait to make sure that mQTLs are always on the right."""
    mock_schema = t.StructType(
        [
            t.StructField("studyLocusId", t.LongType()),
            t.StructField("studyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
            t.StructField("tagVariantId", t.StringType()),
        ]
    )
    expected_schema = t.StructType(
        [
            t.StructField("left_studyLocusId", t.LongType()),
            t.StructField("right_studyLocusId", t.LongType()),
            t.StructField("chromosome", t.StringType()),
        ]
    )
    observed_df = spark.createDataFrame(observed, mock_schema)
    result_df = StudyLocus._overlapping_peaks(observed_df)
    expected_df = spark.createDataFrame(expected, expected_schema)
    assert result_df.collect() == expected_df.collect()


class TestFindOverlapsInLocus:
    """Tests the StudyLocus.find_overlaps_in_locus method."""

    @pytest.mark.parametrize(
        ("distance_from_lead", "expected_locus_size"),
        [
            (25, [3, 3, 2, 2]),
            (5, [2, 0, 0, 0]),
        ],
    )
    def test_filter_locus_by_distance(
        self, distance_from_lead: int, expected_locus_size: list
    ) -> None:
        """Test filtering of locus by distance."""
        observed_df = self.mock_sl.filter_locus_by_distance(distance_from_lead).df
        observed_locus_size = (
            observed_df.select(f.size("locus").alias("locus_size"))
            .toPandas()["locus_size"]
            .to_list()
        )
        assert observed_locus_size == expected_locus_size

    @pytest.mark.parametrize(
        ("distance_between_leads", "expected_overlapping_studylocus"),
        [
            (25, [(1, 2), (2, 4)]),
            (5, []),
        ],
    )
    def test_get_loci_to_overlap(
        self: TestFindOverlapsInLocus,
        distance_between_leads: int,
        expected_overlapping_studylocus: list,
    ) -> None:
        """Test getting loci to overlap."""
        observed_df = self.mock_sl._get_loci_to_overlap(distance_between_leads)
        observed_overlapping_studylocus = [
            (row.left_studyLocusId, row.right_studyLocusId)
            for row in observed_df.collect()
        ]
        assert observed_overlapping_studylocus == expected_overlapping_studylocus

    # def test_find_overlaps_in_locus(self):
    #     pass

    @pytest.fixture(autouse=True)
    def _setup(
        self: TestFindOverlapsInLocus,
        spark: SparkSession,
    ) -> None:
        """Prepares the data for the tests."""
        mock_sl_data = [
            (
                1,
                "varA",
                "traitA",
                "chr10",
                10,
                [
                    {"variantId": "10_2_X_X"},
                    {"variantId": "10_6_X_X"},
                    {"variantId": "10_15_X_X"},
                ],
            ),
            (
                2,
                "varB",
                "traitB",
                "chr10",
                30,
                [
                    {"variantId": "10_15_X_X"},
                    {"variantId": "10_20_X_X"},
                    {"variantId": "10_40_X_X"},
                ],
            ),
            # outside of all windows
            (
                3,
                "varC",
                "traitC",
                "chr10",
                100,
                [{"variantId": "10_90_X_X"}, {"variantId": "10_110_X_X"}],
            ),
            # outside of the first window
            (
                4,
                "varD",
                "traitD",
                "chr10",
                50,
                [{"variantId": "10_40_X_X"}, {"variantId": "10_60_X_X"}],
            ),
        ]
        mock_sl_schema = t.StructType(
            [
                t.StructField("studyLocusId", t.LongType(), False),
                t.StructField("variantId", t.StringType(), False),
                t.StructField("studyId", t.StringType(), False),
                t.StructField("chromosome", t.StringType(), True),
                t.StructField("position", t.IntegerType(), True),
                t.StructField(
                    "locus",
                    t.ArrayType(
                        t.StructType([t.StructField("variantId", t.StringType(), True)])
                    ),
                    True,
                ),
            ]
        )
        self.mock_sl = StudyLocus(
            _df=spark.createDataFrame(mock_sl_data, mock_sl_schema)
        )
