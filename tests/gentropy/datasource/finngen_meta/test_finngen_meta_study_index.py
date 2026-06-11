"""Tests for FinnGen meta-analysis study-index generation."""

from unittest.mock import MagicMock

import pytest

from gentropy import Session
from gentropy.dataset.study_index import MetaAnalysisStudyIndex
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen_meta import FinnGenMetaRelease, MetaAnalysisType
from gentropy.datasource.finngen_meta.study_index import FinnGenMetaManifest

_THREE_WAY_MANIFEST_PATH = (
    "tests/gentropy/data_samples/finngen_ukbb_mvp_meta_manifest.tsv"
)
_TWO_WAY_MANIFEST_PATH = "tests/gentropy/data_samples/finngen_ukbb_meta_manifest.tsv"
_RELEASE = FinnGenMetaRelease(release="R12")


class TestFinnGenMetaStudyIndex:
    """Test that FinnGenMetaManifest.from_source produces a valid study index."""

    @pytest.fixture
    def mock_efo_mapping(self) -> EFOMapping:
        """Pass-through EFO mapping mock; returns the study index unchanged."""
        mock = MagicMock(spec=EFOMapping)
        mock.annotate_study_index.side_effect = lambda study_index, **kwargs: (
            study_index
        )
        return mock

    @pytest.fixture
    def three_way_manifest(
        self, session: Session, mock_efo_mapping: EFOMapping
    ) -> MetaAnalysisStudyIndex:
        """Load THREE_WAY manifest from the sample TSV."""
        return FinnGenMetaManifest.from_source(
            session,
            _THREE_WAY_MANIFEST_PATH,
            meta_analysis_type=MetaAnalysisType.THREE_WAY,
            release=_RELEASE,
            efo_mapping=mock_efo_mapping,
        )

    def test_ld_population_structure_computed(
        self, three_way_manifest: MetaAnalysisStudyIndex
    ) -> None:
        """Test that ldPopulationStructure is derived from discoverySamples ancestries.

        THREE_WAY ancestries Finnish/European/African/Admixed American must map to
        fin/nfe/afr/amr respectively.
        """
        rows = three_way_manifest.df.select("ldPopulationStructure").collect()
        assert len(rows) == 4
        for row in rows:
            ld_pops = {entry["ldPopulation"] for entry in row["ldPopulationStructure"]}
            assert ld_pops == {"fin", "nfe", "afr", "amr"}, (
                f"Expected LD populations fin/nfe/afr/amr, got {ld_pops}"
            )

    @pytest.fixture
    def two_way_manifest(
        self, session: Session, mock_efo_mapping: EFOMapping
    ) -> MetaAnalysisStudyIndex:
        """Load TWO_WAY manifest from the sample TSV."""
        return FinnGenMetaManifest.from_source(
            session,
            _TWO_WAY_MANIFEST_PATH,
            meta_analysis_type=MetaAnalysisType.TWO_WAY,
            release=_RELEASE,
            efo_mapping=mock_efo_mapping,
        )

    def test_two_way_ld_population_structure_computed(
        self, two_way_manifest: MetaAnalysisStudyIndex
    ) -> None:
        """Test that TWO_WAY discoverySamples map to non-null LD populations.

        Regression test: TWO_WAY ancestries must use the human-readable labels
        Finnish/European (keys of the LD-panel map) so they resolve to fin/nfe
        rather than null.
        """
        rows = two_way_manifest.df.select("ldPopulationStructure").collect()
        assert rows
        for row in rows:
            ld_pops = {entry["ldPopulation"] for entry in row["ldPopulationStructure"]}
            assert None not in ld_pops, (
                f"TWO_WAY LD populations must not be null, got {ld_pops}"
            )
            assert ld_pops == {"fin", "nfe"}, (
                f"Expected LD populations fin/nfe, got {ld_pops}"
            )

    def test_efo_mapping_called(
        self, session: Session, mock_efo_mapping: EFOMapping
    ) -> None:
        """Test that annotate_study_index is called with the release identifier."""
        FinnGenMetaManifest.from_source(
            session,
            _THREE_WAY_MANIFEST_PATH,
            meta_analysis_type=MetaAnalysisType.THREE_WAY,
            release=_RELEASE,
            efo_mapping=mock_efo_mapping,
        )
        annotate_mock = mock_efo_mapping.annotate_study_index
        annotate_mock.assert_called_once()  # type: ignore[attr-defined]
        assert annotate_mock.call_args.kwargs["finngen_release"] == _RELEASE.release  # type: ignore[attr-defined]
