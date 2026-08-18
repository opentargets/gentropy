"""Test eQTL Catalogue ingestion step."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, Row

from gentropy.common.session import Session
from gentropy.eqtl_catalogue import EqtlCatalogueStep


class TestEqtlCatalogueStep:
    """Test suite for the eQTL Catalogue ingestion step."""

    @pytest.fixture
    def study_index_df(self, session: Session) -> DataFrame:
        """Minimal DataFrame standing in for the StudyIndex payload that gets written out."""
        return session.spark.createDataFrame([Row(studyId="s1", traitFromSource="t1")])

    @pytest.fixture
    def credible_set_df(self, session: Session) -> DataFrame:
        """Minimal DataFrame standing in for the StudyLocus payload, shaped for repartition/sort."""
        return session.spark.createDataFrame(
            [
                Row(studyId="s1", chromosome="1", variantId="1_1_A_T"),
                Row(studyId="s1", chromosome="2", variantId="2_2_C_G"),
            ]
        )

    @pytest.mark.step_test
    @patch("gentropy.eqtl_catalogue.EqtlCatalogueFinemapping")
    @patch("gentropy.eqtl_catalogue.EqtlCatalogueStudyIndex")
    def test_step(
        self,
        study_index_mock: MagicMock,
        finemapping_mock: MagicMock,
        session: Session,
        tmp_path: Path,
        study_index_df: DataFrame,
        credible_set_df: DataFrame,
    ) -> None:
        """The step should extract, join/transform and write study index and credible sets.

        Every collaborator is mocked out, so this test asserts the *orchestration*: which
        classmethods get called, with what arguments, that intermediate DataFrames are
        persisted and unpersisted at the right points, and that outputs land on disk.
        """
        eqtl_catalogue_dataset_metadata_path = "metadata_path"
        credible_set_input_glob = "credible_set_glob"
        lbf_variable_input_glob = "lbf_glob"
        study_index_output_path = (tmp_path / "study_index").as_posix()
        credible_set_output_path = (tmp_path / "credible_set").as_posix()
        lead_pvalue_threshold = 1e-3
        mqtl_quantification_methods_blacklist = ["ge"]

        # Raw DataFrames returned by the read_* methods. Each supports the same
        # .persist()/.unpersist() chaining a real Spark DataFrame would.
        studies_metadata_mock = MagicMock(name="studies_metadata_df")
        studies_metadata_mock.persist.return_value = studies_metadata_mock

        credible_sets_mock = MagicMock(name="credible_sets_df")
        credible_sets_mock.persist.return_value = credible_sets_mock

        lbf_mock = MagicMock(name="lbf_df")
        lbf_mock.persist.return_value = lbf_mock

        processed_susie_mock = MagicMock(name="processed_susie_df")
        processed_susie_mock.persist.return_value = processed_susie_mock

        study_index_mock.read_studies_from_source.return_value = studies_metadata_mock
        finemapping_mock.read_credible_set_from_source.return_value = credible_sets_mock
        finemapping_mock.read_lbf_from_source.return_value = lbf_mock
        finemapping_mock.parse_susie_results.return_value = processed_susie_mock

        # Dataset-like return values (StudyIndex/StudyLocus). Each chained method
        # returns the same mock, ending in a real `.df` so the write actually runs.
        study_index_dataset = MagicMock(name="study_index_dataset")
        study_index_dataset.coalesce.return_value = study_index_dataset
        study_index_dataset.df = study_index_df
        study_index_mock.from_susie_results.return_value = study_index_dataset

        study_locus_dataset = MagicMock(name="study_locus_dataset")
        study_locus_dataset.validate_lead_pvalue.return_value = study_locus_dataset
        study_locus_dataset.df = credible_set_df
        finemapping_mock.from_susie_results.return_value = study_locus_dataset

        EqtlCatalogueStep(
            session=session,
            eqtl_catalogue_dataset_metadata_path=eqtl_catalogue_dataset_metadata_path,
            credible_set_input_glob=credible_set_input_glob,
            lbf_variable_input_glob=lbf_variable_input_glob,
            study_index_output_path=study_index_output_path,
            credible_set_output_path=credible_set_output_path,
            lead_pvalue_threshold=lead_pvalue_threshold,
            mqtl_quantification_methods_blacklist=mqtl_quantification_methods_blacklist,
        )

        # --- Extract ---
        study_index_mock.read_studies_from_source.assert_called_once_with(
            eqtl_catalogue_dataset_metadata_path,
            mqtl_quantification_methods_blacklist,
            session=session,
        )
        studies_metadata_mock.persist.assert_called_once()
        studies_metadata_mock.count.assert_called_once()

        finemapping_mock.read_credible_set_from_source.assert_called_once_with(
            credible_set_input_glob,
            session=session,
        )
        credible_sets_mock.persist.assert_called_once()
        credible_sets_mock.count.assert_called_once()

        finemapping_mock.read_lbf_from_source.assert_called_once_with(
            lbf_variable_input_glob,
            session=session,
        )
        lbf_mock.persist.assert_called_once()
        lbf_mock.count.assert_called_once()

        # --- Transform ---
        finemapping_mock.parse_susie_results.assert_called_once_with(
            credible_sets_mock, lbf_mock, studies_metadata_mock
        )
        processed_susie_mock.persist.assert_called_once()
        processed_susie_mock.count.assert_called_once()

        # The raw inputs are freed as soon as the join that consumes them is materialised.
        studies_metadata_mock.unpersist.assert_called_once()
        credible_sets_mock.unpersist.assert_called_once()
        lbf_mock.unpersist.assert_called_once()

        # --- Study index write ---
        study_index_mock.from_susie_results.assert_called_once_with(
            processed_susie_mock
        )
        study_index_dataset.coalesce.assert_called_once_with(1)
        assert Path(study_index_output_path).exists()
        written_study_index = session.spark.read.parquet(study_index_output_path)
        assert written_study_index.count() == study_index_df.count()

        # --- Credible set write ---
        finemapping_mock.from_susie_results.assert_called_once_with(
            processed_susie_mock
        )
        study_locus_dataset.validate_lead_pvalue.assert_called_once_with(
            pvalue_cutoff=lead_pvalue_threshold
        )
        assert Path(credible_set_output_path).exists()
        written_credible_sets = session.spark.read.parquet(credible_set_output_path)
        assert written_credible_sets.count() == credible_set_df.count()

        # The fully processed, joined DataFrame is only freed once both writes are done.
        processed_susie_mock.unpersist.assert_called_once()
