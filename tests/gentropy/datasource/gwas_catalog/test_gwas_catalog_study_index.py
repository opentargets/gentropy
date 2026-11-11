"""Test GWASCatalogStudyIndex."""

from __future__ import annotations

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from gentropy.datasource.gwas_catalog.study_index import (
    StudyIndexGWASCatalog,
    StudyIndexGWASCatalogParser,
)


def test_annotate_discovery_sample_sizes(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
) -> None:
    """Test annotate discovery sample sizes."""
    mock_study_index_gwas_catalog.df = mock_study_index_gwas_catalog.df.drop(
        "nCases", "nControls", "nSamples"
    )
    assert isinstance(
        mock_study_index_gwas_catalog.annotate_discovery_sample_sizes(),
        StudyIndexGWASCatalog,
    )


@pytest.mark.parametrize(
    ("initial_sample_size", "expected"),
    [
        pytest.param(
            "1,000 European ancestry cases, 2,000 European ancestry controls",
            (1_000, 2_000, 3_000),
            id="cases and controls",
        ),
        pytest.param(
            "10 African ancestry cases, 20 African ancestry controls, "
            "30 Greater Middle Eastern (Middle Eastern, North African or Persian) cases, "
            "40 Greater Middle Eastern (Middle Eastern, North African or Persian) controls",
            (40, 60, 100),
            id="comma inside population label",
        ),
        pytest.param(
            "5,000 European ancestry individuals", (0, 0, 5_000), id="quantitative"
        ),
    ],
)
def test_annotate_discovery_sample_sizes_values(
    mock_study_index_gwas_catalog: StudyIndexGWASCatalog,
    initial_sample_size: str,
    expected: tuple[int, int, int],
) -> None:
    """Test that cases, controls and samples are summed from the initial sample size."""
    mock_study_index_gwas_catalog.df = (
        mock_study_index_gwas_catalog.df.limit(1)
        .drop("nCases", "nControls", "nSamples")
        .withColumn("initialSampleSize", f.lit(initial_sample_size))
    )
    row = (
        mock_study_index_gwas_catalog.annotate_discovery_sample_sizes()
        .df.select("nCases", "nControls", "nSamples")
        .first()
    )
    assert row is not None
    assert tuple(row) == expected


def test_parse_study_table(sample_gwas_catalog_studies: DataFrame) -> None:
    """Test parse study table."""
    assert isinstance(
        StudyIndexGWASCatalogParser._parse_study_table(sample_gwas_catalog_studies),
        StudyIndexGWASCatalog,
    )


def test_study_index_from_source(
    sample_gwas_catalog_studies: DataFrame,
    sample_gwas_catalog_ancestries_lut: DataFrame,
) -> None:
    """Test study index from source."""
    assert isinstance(
        StudyIndexGWASCatalogParser.from_source(
            sample_gwas_catalog_studies,
            sample_gwas_catalog_ancestries_lut,
        ),
        StudyIndexGWASCatalog,
    )
