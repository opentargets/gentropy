"""Test suite for the LDMatrixInterface class in the gentropy package."""

from unittest.mock import MagicMock, patch

import pytest

from gentropy.method.ld_matrix_interface import LDMatrixInterface


@pytest.fixture
def default_ld_matrix_paths() -> dict[str, str]:
    """Default LD matrix paths for testing."""
    return {
        "pan_ukbb_bm_path": "gs://panukbb-ld-matrixes/UKBB.{POP}.ldadj",
        "ukbb_annotation_path": "gs://panukbb-ld-matrixes/UKBB.{POP}.aligned.parquet",
        "ld_matrix_template": "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm",
        "ld_index_raw_template": "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht",
        "liftover_ht_path": "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht",
        "grch37_to_grch38_chain_path": "gs://hail-common/references/grch37_to_grch38.over.chain.gz",
    }


@pytest.fixture
def override_ld_matrix_paths() -> dict[str, str]:
    """Override LD matrix paths for testing."""
    return {
        "pan_ukbb_bm_path": "/path/to/local/UKBB.{POP}.ldadj",
        "ukbb_annotation_path": "/path/to/local/UKBB.{POP}.aligned.parquet",
        "ld_matrix_template": "/path/to/local/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm",
        "ld_index_raw_template": "/path/to/local/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht",
        "liftover_ht_path": "/path/to/local/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht",
        "grch37_to_grch38_chain_path": "/path/to/local/grch37_to_grch38.over.chain.gz",
    }


class TestLDMatrixInterfacePanUKBB:
    """Test PanUKBB LD methods for locus boundaries."""

    @pytest.mark.parametrize(
        ("ancestry", "expected_population"),
        [
            ("nfe", "EUR"),
            ("csa", "CSA"),
            ("afr", "AFR"),
            ("EUR", "EUR"),
            ("CSA", "CSA"),
            ("AFR", "AFR"),
        ],
    )
    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_locus_index_boundaries_panukbb_supported_labels(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        ancestry: str,
        expected_population: str,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """PanUKBB ancestries accept pipeline aliases and explicit PanUKBB labels."""
        session = MagicMock()
        mock_study_locus_row: MagicMock = MagicMock()

        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=default_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry=ancestry,
        )

        mock_pan_ukbb_matrix.assert_called_once_with(
            ukbb_annotation_path=default_ld_matrix_paths["ukbb_annotation_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_locus_index_boundaries.assert_called_once_with(
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry=expected_population,
        )

    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_locus_index_boundaries_panukbb_default(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for PanUKBB with default paths."""
        # Setup
        session = MagicMock()
        mock_study_locus_row: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=default_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="nfe",
        )

        # Verify
        mock_pan_ukbb_matrix.assert_called_once_with(
            ukbb_annotation_path=default_ld_matrix_paths["ukbb_annotation_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_locus_index_boundaries.assert_called_once_with(
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="EUR",
        )

    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_locus_index_boundaries_panukbb_override(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for PanUKBB with overridden paths."""
        # Setup
        session = MagicMock()
        mock_study_locus_row: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=override_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="nfe",
        )

        # Verify
        mock_pan_ukbb_matrix.assert_called_once_with(
            ukbb_annotation_path=override_ld_matrix_paths["ukbb_annotation_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_locus_index_boundaries.assert_called_once_with(
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="EUR",
        )

    @pytest.mark.parametrize(
        ("ancestry", "expected_population"),
        [
            ("nfe", "EUR"),
            ("csa", "CSA"),
            ("afr", "AFR"),
            ("EUR", "EUR"),
            ("CSA", "CSA"),
            ("AFR", "AFR"),
        ],
    )
    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_numpy_matrix_panukbb_supported_labels(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        ancestry: str,
        expected_population: str,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """PanUKBB matrix access accepts pipeline aliases and explicit PanUKBB labels."""
        mock_locus_index: MagicMock = MagicMock()

        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=default_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry=ancestry,
        )

        mock_pan_ukbb_matrix.assert_called_once_with(
            pan_ukbb_bm_path=default_ld_matrix_paths["pan_ukbb_bm_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_numpy_matrix.assert_called_once_with(
            locus_index=mock_locus_index,
            ancestry=expected_population,
        )

    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_numpy_matrix_panukbb_default(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for PanUKBB with default paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=default_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="nfe",
        )

        # Verify
        mock_pan_ukbb_matrix.assert_called_once_with(
            pan_ukbb_bm_path=default_ld_matrix_paths["pan_ukbb_bm_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_numpy_matrix.assert_called_once_with(
            locus_index=mock_locus_index,
            ancestry="EUR",
        )

    @patch("gentropy.method.ld_matrix_interface.PanUKBBLDMatrix")
    def test_get_numpy_matrix_panukbb_override(
        self,
        mock_pan_ukbb_matrix: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for PanUKBB with overridden paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=override_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="nfe",
        )

        # Verify
        mock_pan_ukbb_matrix.assert_called_once_with(
            pan_ukbb_bm_path=override_ld_matrix_paths["pan_ukbb_bm_path"]
        )
        mock_pan_ukbb_matrix.return_value.get_numpy_matrix.assert_called_once_with(
            locus_index=mock_locus_index,
            ancestry="EUR",
        )


class TestLDMatrixInterfaceGnomAD:
    """Test GnomAD LD methods for locus boundaries."""

    @patch("gentropy.method.ld_matrix_interface.GnomADLDMatrix")
    @patch("gentropy.method.ld_matrix_interface.f")
    def test_get_locus_index_boundaries_gnomad_default(
        self,
        mock_functions: MagicMock,
        mock_gnomad_matrix: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for GnomAD with default paths."""
        # Setup
        session = MagicMock()
        mock_study_locus_row: MagicMock = MagicMock()
        mock_gnomad_locus_index = MagicMock()
        mock_gnomad_matrix.return_value.get_locus_index_boundaries.return_value = (
            mock_gnomad_locus_index
        )
        mock_functions.col.return_value = MagicMock()
        mock_functions.regexp_replace.return_value = MagicMock()
        mock_functions.lit.return_value = MagicMock()
        mock_functions.concat.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=default_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="eas",
        )

        # Verify
        mock_gnomad_matrix.assert_called_once_with(
            liftover_ht_path=default_ld_matrix_paths["liftover_ht_path"],
            ld_index_raw_template=default_ld_matrix_paths["ld_index_raw_template"],
        )
        mock_gnomad_matrix.return_value.get_locus_index_boundaries.assert_called_once_with(
            study_locus_row=mock_study_locus_row,
            major_population="eas",
        )
        mock_gnomad_locus_index.withColumn.assert_called_once()

    @patch("gentropy.method.ld_matrix_interface.GnomADLDMatrix")
    @patch("gentropy.method.ld_matrix_interface.f")
    def test_get_locus_index_boundaries_gnomad_override(
        self,
        mock_functions: MagicMock,
        mock_gnomad_matrix: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for GnomAD with overridden paths."""
        # Setup
        session = MagicMock()
        mock_study_locus_row: MagicMock = MagicMock()
        mock_gnomad_locus_index = MagicMock()
        mock_gnomad_matrix.return_value.get_locus_index_boundaries.return_value = (
            mock_gnomad_locus_index
        )
        mock_functions.col.return_value = MagicMock()
        mock_functions.regexp_replace.return_value = MagicMock()
        mock_functions.lit.return_value = MagicMock()
        mock_functions.concat.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=override_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="eas",
        )

        # Verify
        mock_gnomad_matrix.assert_called_once_with(
            liftover_ht_path=override_ld_matrix_paths["liftover_ht_path"],
            ld_index_raw_template=override_ld_matrix_paths["ld_index_raw_template"],
        )
        mock_gnomad_matrix.return_value.get_locus_index_boundaries.assert_called_once_with(
            study_locus_row=mock_study_locus_row,
            major_population="eas",
        )
        mock_gnomad_locus_index.withColumn.assert_called_once()

    @patch("gentropy.method.ld_matrix_interface.GnomADLDMatrix")
    def test_get_numpy_matrix_gnomad_default(
        self,
        mock_gnomad_matrix: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for GnomAD with default paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=default_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="eas",
        )

        # Verify
        mock_gnomad_matrix.assert_called_once_with(
            ld_matrix_template=default_ld_matrix_paths["ld_matrix_template"]
        )
        mock_gnomad_matrix.return_value.get_numpy_matrix.assert_called_once_with(
            locus_index=mock_locus_index,
            gnomad_ancestry="eas",
        )

    @patch("gentropy.method.ld_matrix_interface.GnomADLDMatrix")
    def test_get_numpy_matrix_gnomad_override(
        self,
        mock_gnomad_matrix: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for GnomAD with overridden paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=override_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="eas",
        )

        # Verify
        mock_gnomad_matrix.assert_called_once_with(
            ld_matrix_template=override_ld_matrix_paths["ld_matrix_template"]
        )
        mock_gnomad_matrix.return_value.get_numpy_matrix.assert_called_once_with(
            locus_index=mock_locus_index,
            gnomad_ancestry="eas",
        )
