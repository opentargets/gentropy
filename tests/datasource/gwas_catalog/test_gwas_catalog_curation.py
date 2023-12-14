"""Test GWAS Catalog study curation logic."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from otg.datasource.gwas_catalog.study_index import StudyIndexGWASCatalog

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class TestGWASCatalogStudyCuration:
    """Test suite for GWAS Catalog study curation logic."""

    @pytest.fixture(scope="class")
    def mock_gwas_study_index(
        self: TestGWASCatalogStudyCuration,
        spark: SparkSession,
    ) -> StudyIndexGWASCatalog:
        """Generate a dataset with a given set of studies with minimalistic schema."""
        # Generating a study index with 7 studies where the first study (s0) has no summary statistics:
        study_data = [
            (f"s{count}", False) if count == 0 else (f"s{count}", True)
            for count in range(0, 7)
        ]
        columns = [
            "projectId",
            "studyType",
            "traitFromSource",
            "publicationTitle",
            "publicationFirstAuthor",
        ]
        return StudyIndexGWASCatalog(
            _df=(
                spark.createDataFrame(study_data, ["studyId", "hasSumstats"]).select(
                    "*", *[f.lit("foo").alias(colname) for colname in columns]
                )
            ),
            _schema=StudyIndexGWASCatalog.get_schema(),
        )

    @pytest.fixture(scope="class")
    def mock_study_curation(
        self: TestGWASCatalogStudyCuration, spark: SparkSession
    ) -> DataFrame:
        """Generate a mocked curation dataset with matching studies."""
        curation_data = [
            ("s2", None, None, None, True),  # Good study
            ("s3", "pQTL", None, None, True),  # Update type
            ("s4", None, "analysis 1", None, True),  # Add analysis flag
            ("s5", None, None, "QC flag", True),  # Add analysis flag
        ]

        curation_columns = [
            "studyId",
            "updateStudyType",
            "upateAnalysisFlags",
            "upateQualityControls",
            "isCurated",
        ]
        return spark.createDataFrame(curation_data, curation_columns)
