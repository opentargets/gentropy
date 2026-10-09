"""Tests GWAS Catalog study splitter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pytest

from gentropy.datasource.gwas_catalog.associations import StudyLocusGWASCatalog
from gentropy.datasource.gwas_catalog.study_index import StudyIndexGWASCatalog
from gentropy.datasource.gwas_catalog.study_splitter import GWASCatalogStudySplitter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_gwas_catalog_splitter_split(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    mock_study_locus_gwas_catalog: StudyLocusGWASCatalog,
) -> None:
    """Test GWASCatalogStudyIndex, GWASCatalogAssociations creation with mock data."""
    d1, d2 = GWASCatalogStudySplitter.split(
        mock_study_index_gwas_catalog, mock_study_locus_gwas_catalog
    )

    assert isinstance(d1, StudyIndexGWASCatalog)
    assert isinstance(d2, StudyLocusGWASCatalog)


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
