"""Test suite for the LDMatrixInterface class in the gentropy package."""

from unittest.mock import MagicMock, patch

import pytest

from gentropy.common.session import Session
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

    @patch("pyspark.sql.DataFrameReader.parquet")
    def test_get_locus_index_boundaries_panukbb_default(
        self, mock_parquet_reader: MagicMock, default_ld_matrix_paths: dict[str, str]
    ) -> None:
        """Test getting locus index boundaries for PanUKBB with default paths."""
        # Setup
        session: Session = Session()
        mock_study_locus_row: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=default_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="nfe",
        )

        # Verify
        mock_parquet_reader.assert_called_once_with(
            default_ld_matrix_paths["ukbb_annotation_path"].format(POP="EUR")
        )

    @patch("pyspark.sql.DataFrameReader.parquet")
    def test_get_locus_index_boundaries_panukbb_override(
        self, mock_parquet_reader: MagicMock, override_ld_matrix_paths: dict[str, str]
    ) -> None:
        """Test getting locus index boundaries for PanUKBB with overridden paths."""
        # Setup
        session: Session = Session()
        mock_study_locus_row: MagicMock = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=override_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="nfe",
        )

        # Verify
        mock_parquet_reader.assert_called_once_with(
            override_ld_matrix_paths["ukbb_annotation_path"].format(POP="EUR")
        )

    @patch("hail.linalg.BlockMatrix.read")
    @patch("gentropy.datasource.pan_ukbb_ld.ld.PanUKBBLDMatrix._get_outer_allele_order")
    @patch("gentropy.datasource.pan_ukbb_ld.ld.PanUKBBLDMatrix._construct_ld_matrix")
    def test_get_numpy_matrix_panukbb_default(
        self,
        mock_construct_ld_matrix: MagicMock,
        mock_get_outer_allele_order: MagicMock,
        mock_block_matrix_read: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for PanUKBB with default paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()
        mock_block_matrix_read.return_value = MagicMock()
        mock_construct_ld_matrix.return_value = MagicMock()
        mock_get_outer_allele_order.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=default_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="nfe",
        )

        # Verify
        mock_block_matrix_read.assert_called_once_with(
            default_ld_matrix_paths["pan_ukbb_bm_path"].format(POP="EUR")
        )

    @patch("hail.linalg.BlockMatrix.read")
    @patch("gentropy.datasource.pan_ukbb_ld.ld.PanUKBBLDMatrix._get_outer_allele_order")
    @patch("gentropy.datasource.pan_ukbb_ld.ld.PanUKBBLDMatrix._construct_ld_matrix")
    def test_get_numpy_matrix_panukbb_override(
        self,
        mock_construct_ld_matrix: MagicMock,
        mock_get_outer_allele_order: MagicMock,
        mock_block_matrix_read: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for PanUKBB with overridden paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()
        mock_block_matrix_read.return_value = MagicMock()
        mock_construct_ld_matrix.return_value = MagicMock()
        mock_get_outer_allele_order.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=override_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="nfe",
        )

        # Verify
        mock_block_matrix_read.assert_called_once_with(
            override_ld_matrix_paths["pan_ukbb_bm_path"].format(POP="EUR")
        )


class TestLDMatrixInterfaceGnomAD:
    """Test GnomAD LD methods for locus boundaries."""

    @patch("hail.read_table")
    @patch("gentropy.datasource.gnomad.ld.GnomADLDMatrix._filter_liftover_by_locus")
    def test_get_locus_index_boundaries_gnomad_default(
        self,
        mock_filter_liftover: MagicMock,
        mock_read_table: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for GnomAD with default paths."""
        # Setup
        session: Session = Session()
        mock_study_locus_row: MagicMock = MagicMock()
        mock_filter_liftover.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=default_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="eas",
        )

        # Verify
        assert mock_read_table.call_count == 2
        assert (
            mock_read_table.call_args_list[0][0][0]
            == default_ld_matrix_paths["liftover_ht_path"]
        )
        assert mock_read_table.call_args_list[1][0][0] == default_ld_matrix_paths[
            "ld_index_raw_template"
        ].format(POP="eas")

    @patch("hail.read_table")
    @patch("gentropy.datasource.gnomad.ld.GnomADLDMatrix._filter_liftover_by_locus")
    def test_get_locus_index_boundaries_gnomad_override(
        self,
        mock_filter_liftover: MagicMock,
        mock_read_table: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting locus index boundaries for GnomAD with overridden paths."""
        # Setup
        session: Session = Session()
        mock_study_locus_row: MagicMock = MagicMock()
        mock_filter_liftover.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_locus_index_boundaries(
            ld_matrix_paths=override_ld_matrix_paths,
            session=session,
            study_locus_row=mock_study_locus_row,
            ancestry="eas",
        )

        # Verify
        assert mock_read_table.call_count == 2
        assert (
            mock_read_table.call_args_list[0][0][0]
            == override_ld_matrix_paths["liftover_ht_path"]
        )
        assert mock_read_table.call_args_list[1][0][0] == override_ld_matrix_paths[
            "ld_index_raw_template"
        ].format(POP="eas")

    @patch("hail.linalg.BlockMatrix.read")
    def test_get_numpy_matrix_gnomad_default(
        self,
        mock_block_matrix_read: MagicMock,
        default_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for GnomAD with default paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()
        mock_block_matrix_read.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=default_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="eas",
        )

        # Verify
        mock_block_matrix_read.assert_called_once_with(
            default_ld_matrix_paths["ld_matrix_template"].format(POP="eas")
        )

    @patch("hail.linalg.BlockMatrix.read")
    def test_get_numpy_matrix_gnomad_override(
        self,
        mock_block_matrix_read: MagicMock,
        override_ld_matrix_paths: dict[str, str],
    ) -> None:
        """Test getting numpy matrix for GnomAD with overridden paths."""
        # Setup
        mock_locus_index: MagicMock = MagicMock()
        mock_block_matrix_read.return_value = MagicMock()

        # Exectue
        LDMatrixInterface.get_numpy_matrix(
            ld_matrix_paths=override_ld_matrix_paths,
            locus_index=mock_locus_index,
            ancestry="eas",
        )

        # Verify
        mock_block_matrix_read.assert_called_once_with(
            override_ld_matrix_paths["ld_matrix_template"].format(POP="eas")
        )
