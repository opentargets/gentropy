"""Test the fine-mapping manifest generator step's orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.finemapping_manifest import GWASCatalogFineMappingManifestGenerator


class TestGWASCatalogFineMappingManifestGeneratorOrchestration:
    """Test that GWASCatalogFineMappingManifestGenerator wires reads, generate, and write correctly.

    All Spark computation is mocked out. Only the step's own orchestration is verified:
    which paths are read, whether generate_manifest is called, and how the TSV is written.
    The generate_manifest logic itself is covered by the integration test.
    """

    @pytest.mark.step_test
    @patch(
        "gentropy.finemapping_manifest.GWASCatalogFineMappingManifestGenerator.generate_manifest"
    )
    @patch("gentropy.finemapping_manifest.FineMappingPlanner")
    @patch("gentropy.finemapping_manifest.StudyIndex")
    def test_orchestration_without_glob(
        self,
        mock_study_index: MagicMock,
        mock_planner: MagicMock,
        mock_generate: MagicMock,
        session: Session,
    ) -> None:
        """Verify reads, generate_manifest call, and TSV write when no glob is provided."""
        si_path = "gs://bucket/study_index"
        planner_path = "gs://bucket/planner"
        out_path = "gs://bucket/manifest.tsv"

        mock_study_index.from_parquet.return_value = mock_study_index
        mock_planner.from_parquet.return_value = mock_planner

        mock_manifest = MagicMock()
        mock_generate.return_value = mock_manifest

        GWASCatalogFineMappingManifestGenerator(
            session=session,
            study_index_path=si_path,
            fine_mapping_planner_path=planner_path,
            output_path=out_path,
        )

        mock_study_index.from_parquet.assert_called_once_with(session, si_path)
        mock_planner.from_parquet.assert_called_once_with(session, planner_path)
        mock_generate.assert_called_once_with(mock_study_index, mock_planner)

        # Output written as a TSV via toPandas
        mock_manifest.df.toPandas.assert_called_once()
        mock_manifest.df.toPandas.return_value.to_csv.assert_called_once_with(
            out_path, sep="\t", index=False
        )

    @pytest.mark.step_test
    @patch(
        "gentropy.finemapping_manifest.GWASCatalogFineMappingManifestGenerator.generate_manifest"
    )
    @patch(
        "gentropy.finemapping_manifest.GWASCatalogFineMappingManifestGenerator._update_study_index_with_sumstat_paths"
    )
    @patch("gentropy.finemapping_manifest.FineMappingPlanner")
    @patch("gentropy.finemapping_manifest.StudyIndex")
    def test_orchestration_with_glob(
        self,
        mock_study_index: MagicMock,
        mock_planner: MagicMock,
        mock_update: MagicMock,
        mock_generate: MagicMock,
        session: Session,
    ) -> None:
        """When a glob is provided, list_hadoop_paths is called and the study index is updated."""
        si_path = "gs://bucket/study_index"
        planner_path = "gs://bucket/planner"
        out_path = "gs://bucket/manifest.tsv"
        glob = "gs://bucket/sumstats/*.tsv.gz"
        resolved_paths = ["gs://bucket/sumstats/GCST001.tsv.gz"]

        mock_study_index.from_parquet.return_value = mock_study_index
        mock_planner.from_parquet.return_value = mock_planner
        updated_si = MagicMock()
        mock_update.return_value = updated_si
        mock_generate.return_value = MagicMock()

        with patch.object(
            session, "list_hadoop_paths", return_value=resolved_paths
        ) as mock_list:
            GWASCatalogFineMappingManifestGenerator(
                session=session,
                study_index_path=si_path,
                fine_mapping_planner_path=planner_path,
                output_path=out_path,
                summary_statistics_glob=glob,
            )

        mock_list.assert_called_once_with(glob)
        mock_update.assert_called_once_with(mock_study_index, resolved_paths)
        mock_generate.assert_called_once_with(updated_si, mock_planner)
