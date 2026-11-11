"""Tests GWAS Catalog study splitter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pytest

from gentropy.datasource.gwas_catalog.study_index import StudyIndexGWASCatalog
from gentropy.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_gwas_catalog_splitter_split(
    spark: SparkSession, mock_study_index_gwas_catalog: StudyIndexGWASCatalog
) -> None:
    """A study whose associations describe two traits is split, and so are its associations."""
    study_id = mock_study_index_gwas_catalog.df.first().studyId
    associations = spark.createDataFrame(
        [
            ("varA", study_id, "Endometrial cancer|no_pvalue_text|EFO_1001512"),
            ("varB", study_id, "Uterine carcinoma|no_pvalue_text|EFO_0002919"),
        ],
        "variantId string, studyId string, subStudyDescription string",
    )

    studies, split = GWASCatalogStudySplitter.split(
        mock_study_index_gwas_catalog, associations
    )

    assert isinstance(studies, StudyIndexGWASCatalog)
    assert "subStudyDescription" not in split.columns
    assert sorted((row.variantId, row.studyId) for row in split.collect()) == [
        ("varA", study_id),
        ("varB", f"{study_id}_2"),
    ]


@pytest.mark.parametrize(
    "observed, expected",
    [
        # Test 1 - it shouldn't split
        (
            # observed - 2 associations with the same subStudy annotation
            [
                (
                    "varA",
                    "GCST003436",
                    "Endometrial cancer|no_pvalue_text|EFO_1001512",
                ),
                (
                    "varB",
                    "GCST003436",
                    "Endometrial cancer|no_pvalue_text|EFO_1001512",
                ),
            ],
            # expected - 2 associations with the same unsplit updatedStudyId
            [
                ("GCST003436",),
                ("GCST003436",),
            ],
        ),
        # Test 2 - it should split
        (
            # observed - 2 associations with the different   subStudy annotation
            [
                (
                    "varA",
                    "GCST003436",
                    "Endometrial cancer|no_pvalue_text|EFO_1001512",
                ),
                (
                    "varB",
                    "GCST003436",
                    "Uterine carcinoma|no_pvalue_text|EFO_0002919",
                ),
            ],
            # expected - 2 associations with the same unsplit updatedStudyId
            [
                ("GCST003436",),
                ("GCST003436_2",),
            ],
        ),
    ],
)
def test__resolve_study_id(
    spark: SparkSession, observed: list[Any], expected: list[Any]
) -> None:
    """Test _resolve_study_id."""
    observed_df = spark.createDataFrame(
        observed, schema=["variantId", "studyId", "subStudyDescription"]
    ).select(
        GWASCatalogStudySplitter._resolve_study_id(
            f.col("studyId"), f.col("subStudyDescription").alias("updatedStudyId")
        )
    )
    expected_df = spark.createDataFrame(expected, schema=["updatedStudyId"])
    assert observed_df.collect() == expected_df.collect()
