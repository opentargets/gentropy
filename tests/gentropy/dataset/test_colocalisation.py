"""Test colocalisation dataset."""

from __future__ import annotations

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus


def test_colocalisation_creation(mock_colocalisation: Colocalisation) -> None:
    """Test colocalisation creation with mock data."""
    assert isinstance(mock_colocalisation, Colocalisation)


def test_append_right_study_metadata(
    mock_colocalisation: Colocalisation,
    mock_study_locus: StudyLocus,
    mock_study_index: StudyIndex,
    metadata_cols: list[str] | None = None,
) -> None:
    """Test appending right study metadata."""
    if metadata_cols is None:
        metadata_cols = ["studyType"]
    expected_extra_col = ["rightStudyType", "rightStudyId"]
    res_df = mock_colocalisation.append_right_study_metadata(
        mock_study_locus, mock_study_index, metadata_cols
    )
    for col in expected_extra_col:
        assert col in res_df.columns, f"Column {col} not found in result DataFrame."


def test_extract_maximum_coloc_probability_per_region_and_gene(
    mock_colocalisation: Colocalisation,
    mock_study_locus: StudyLocus,
    mock_study_index: StudyIndex,
    filter_by_colocalisation_method: str | None = None,
) -> None:
    """Test extracting maximum coloc probability per region and gene returns a dataframe with the correct columns: studyLocusId, geneId, h4."""
    filter_by_colocalisation_method = filter_by_colocalisation_method or "Coloc"
    res_df = mock_colocalisation.extract_maximum_coloc_probability_per_region_and_gene(
        mock_study_locus, mock_study_index, filter_by_colocalisation_method
    )
    expected_cols = ["studyLocusId", "geneId", "h4"]
    for col in expected_cols:
        assert col in res_df.columns, f"Column {col} not found in result DataFrame."
