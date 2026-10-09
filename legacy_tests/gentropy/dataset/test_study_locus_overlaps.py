"""Test colocalisation dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pyspark.sql.types as t
import pytest

from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_study_locus_overlap_creation(
    mock_study_locus_overlap: StudyLocusOverlap,
) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_study_locus_overlap, StudyLocusOverlap)


def test_study_locus_overlap_from_associations(mock_study_locus: StudyLocus) -> None:
    """Test colocalisation creation from mock associations."""
    overlaps = StudyLocusOverlap.from_associations(mock_study_locus)
    assert isinstance(overlaps, StudyLocusOverlap)


@pytest.mark.parametrize(
    "scenario",
    [
        {
            "id": "default_mixed_gwas_qtl_and_gwas_gwas",
            "rows": [
                # Three loci sharing same variant V1 on chr1
                {
                    "studyLocusId": "10",
                    "studyId": "GWAS1",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "region": None,
                    "tagVariantId": "V1",
                },
                {
                    "studyLocusId": "05",
                    "studyId": "GWAS2",
                    "studyType": "gwas",
                    "chromosome": "1",
                    "region": None,
                    "tagVariantId": "V1",
                },
                {
                    "studyLocusId": "30",
                    "studyId": "EQTL1",
                    "studyType": "eqtl",
                    "chromosome": "1",
                    "region": None,
                    "tagVariantId": "V1",
                },
            ],
            # Expected:
            # GWAS-GWAS pair: left id must be larger (10 > 05) so (10,05)
            # GWAS-QTL pairs: both GWAS loci vs EQTL
            "expected": [
                {
                    "leftStudyLocusId": "10",
                    "rightStudyLocusId": "05",
                    "rightStudyType": "gwas",
                    "chromosome": "1",
                },
                {
                    "leftStudyLocusId": "10",
                    "rightStudyLocusId": "30",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                },
                {
                    "leftStudyLocusId": "05",
                    "rightStudyLocusId": "30",
                    "rightStudyType": "eqtl",
                    "chromosome": "1",
                },
            ],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "gwas_v_qtl_only",
            "rows": [
                {
                    "studyLocusId": "10",
                    "studyId": "GWAS1",
                    "studyType": "gwas",
                    "chromosome": "2",
                    "region": None,
                    "tagVariantId": "X",
                },
                {
                    "studyLocusId": "11",
                    "studyId": "GWAS2",
                    "studyType": "gwas",
                    "chromosome": "2",
                    "region": None,
                    "tagVariantId": "X",
                },
                {
                    "studyLocusId": "20",
                    "studyId": "EQTL1",
                    "studyType": "eqtl",
                    "chromosome": "2",
                    "region": None,
                    "tagVariantId": "X",
                },
            ],
            # No GWAS-GWAS pair expected
            "expected": [
                {
                    "leftStudyLocusId": "10",
                    "rightStudyLocusId": "20",
                    "rightStudyType": "eqtl",
                    "chromosome": "2",
                },
                {
                    "leftStudyLocusId": "11",
                    "rightStudyLocusId": "20",
                    "rightStudyType": "eqtl",
                    "chromosome": "2",
                },
            ],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": True,
        },
        {
            "id": "restrict_right_studies_qtl",
            "rows": [
                {
                    "studyLocusId": "1",
                    "studyId": "GWAS_A",
                    "studyType": "gwas",
                    "chromosome": "3",
                    "region": None,
                    "tagVariantId": "Z",
                },
                {
                    "studyLocusId": "2",
                    "studyId": "GWAS_B",
                    "studyType": "gwas",
                    "chromosome": "3",
                    "region": None,
                    "tagVariantId": "Z",
                },
                {
                    "studyLocusId": "3",
                    "studyId": "EQTL_B",
                    "studyType": "eqtl",
                    "chromosome": "3",
                    "region": None,
                    "tagVariantId": "Z",
                },
            ],
            # Restrict right to EQTL_B only; left must not be in restrict list
            "expected": [
                {
                    "leftStudyLocusId": "1",
                    "rightStudyLocusId": "3",
                    "rightStudyType": "eqtl",
                    "chromosome": "3",
                },
                {
                    "leftStudyLocusId": "2",
                    "rightStudyLocusId": "3",
                    "rightStudyType": "eqtl",
                    "chromosome": "3",
                },
            ],
            "restrict_right_studies": ["EQTL_B"],
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "restrict_right_studies_gwas_right",
            "rows": [
                {
                    "studyLocusId": "10",
                    "studyId": "GWAS_MAIN",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "region": None,
                    "tagVariantId": "SNP1",
                },
                {
                    "studyLocusId": "05",
                    "studyId": "GWAS_REF",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "region": None,
                    "tagVariantId": "SNP1",
                },
                {
                    "studyLocusId": "07",
                    "studyId": "GWAS_REF",
                    "studyType": "gwas",
                    "chromosome": "4",
                    "region": None,
                    "tagVariantId": "SNP2",
                },
            ],
            # Only overlaps where right.studyId in restrict list (GWAS_REF) and left not in it.
            # Two rows with GWAS_REF on SNP1 produce a single distinct pair (10,05).
            "expected": [
                {
                    "leftStudyLocusId": "10",
                    "rightStudyLocusId": "05",
                    "rightStudyType": "gwas",
                    "chromosome": "4",
                },
            ],
            "restrict_right_studies": ["GWAS_REF"],
            "gwas_v_qtl_overlap_only": False,
        },
        {
            "id": "no_overlaps",
            "rows": [
                {
                    "studyLocusId": "1",
                    "studyId": "GWAS_A",
                    "studyType": "gwas",
                    "chromosome": "5",
                    "region": None,
                    "tagVariantId": "V1",
                },
                {
                    "studyLocusId": "2",
                    "studyId": "EQTL_X",
                    "studyType": "eqtl",
                    "chromosome": "5",
                    "region": None,
                    "tagVariantId": "V2",
                },
            ],
            "expected": [],
            "restrict_right_studies": None,
            "gwas_v_qtl_overlap_only": False,
        },
    ],
    ids=lambda s: s["id"],
)
def test_overlapping_peaks_join_conditions(
    spark: SparkSession, scenario: dict[str, Any]
) -> None:
    """Comprehensive tests for _overlapping_peaks join-condition branches."""
    schema = t.StructType(
        [
            t.StructField("studyLocusId", t.StringType()),
            t.StructField("studyId", t.StringType()),
            t.StructField("studyType", t.StringType()),
            t.StructField("chromosome", t.StringType()),
            t.StructField("region", t.StringType()),
            t.StructField("tagVariantId", t.StringType()),
        ]
    )
    df = spark.createDataFrame(scenario["rows"], schema=schema)

    result = StudyLocus._overlapping_peaks(
        df,
        restrict_right_studies=scenario["restrict_right_studies"],
        gwas_v_qtl_overlap_only=scenario["gwas_v_qtl_overlap_only"],
    ).select("leftStudyLocusId", "rightStudyLocusId", "rightStudyType", "chromosome")

    # Collect and compare as sets (ordering not guaranteed)
    observed = {tuple(r.asDict().items()) for r in result.collect()}
    expected_df = spark.createDataFrame(
        scenario["expected"],
        t.StructType(
            [
                t.StructField("leftStudyLocusId", t.StringType()),
                t.StructField("rightStudyLocusId", t.StringType()),
                t.StructField("rightStudyType", t.StringType()),
                t.StructField("chromosome", t.StringType()),
            ]
        ),
    )
    expected = {tuple(r.asDict().items()) for r in expected_df.collect()}

    assert observed == expected, (
        f"Scenario {scenario['id']} failed.\nObserved: {observed}\nExpected: {expected}"
    )

    # Additional invariant checks
    if scenario["gwas_v_qtl_overlap_only"]:
        # Ensure no GWAS-GWAS pairs
        assert result.filter(f.col("rightStudyType") == "gwas").count() == 0, (
            "GWAS-GWAS pair leaked in gwas_v_qtl_overlap_only mode."
        )

    if scenario["restrict_right_studies"] is not None:
        # Ensure all right studyIds are in restriction list
        right_ids = (
            df.alias("raw")
            .join(
                result,
                on=f.col("raw.studyLocusId") == f.col("rightStudyLocusId"),
                how="inner",
            )
            .select("studyId")
            .distinct()
            .rdd.flatMap(lambda r: r)
            .collect()
        )
        assert set(right_ids).issubset(set(scenario["restrict_right_studies"])), (
            "Right side includes studyId outside restrict_right_studies."
        )


class TestStudyLocusOverlap:
    """Test the overlapping of StudyLocus dataset."""

    @pytest.fixture(autouse=True)
    def setup(
        self: TestStudyLocusOverlap, study_locus_sample_for_colocalisation: StudyLocus
    ) -> None:
        """Get sample dataset."""
        # Store imput dataset:
        self.study_locus = study_locus_sample_for_colocalisation

        # Call locus overlap:
        self.overlaps = study_locus_sample_for_colocalisation.find_overlaps()

    def test_coloc_return_type(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert isinstance(self.overlaps, StudyLocusOverlap)

    def test_coloc_not_null(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert self.overlaps.df.count() != 0

    def test_coloc_study_type_not_null(self: TestStudyLocusOverlap) -> None:
        """Test get_schema."""
        assert self.overlaps.filter(f.col("rightStudyType").isNull()).df.count() == 0
