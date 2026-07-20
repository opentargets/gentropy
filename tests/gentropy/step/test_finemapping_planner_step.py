"""Test the fine-mapping plan generator step's orchestration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from gentropy import Session
from gentropy.common.types import LDPopulation
from gentropy.dataset.study_index import StudyAnalysisFlag, StudyQualityCheck
from gentropy.finemapping_planner import FineMappingPlanGeneratorStep


class TestFineMappingPlanGeneratorStepOrchestration:
    """Test that FineMappingPlanGeneratorStep wires the study index, constraint sets and write path correctly.

    This test mocks out MultiSuSiEConstraintSet entirely (no real Spark computation),
    so it only proves the step's own orchestration - which constraint set(s) it builds,
    with what configuration, and how it combines/writes the resolved plan(s). The real
    write path against genuine data is covered separately in the mocked integration test.
    """

    @pytest.mark.step_test
    @patch("gentropy.finemapping_planner.MultiSuSiEConstraintSet")
    @patch("gentropy.finemapping_planner.StudyIndex")
    def test_step_orchestration(
        self,
        study_index: MagicMock,
        multi_susie_constraint_set: MagicMock,
        session: Session,
    ) -> None:
        """Test that the step reads the study index, builds MultiSuSiE, resolves and writes the plan."""
        input_path = "input_study_index_path"
        output_path = "output_fine_mapping_plan_path"

        study_index.from_parquet = MagicMock(return_value=study_index)

        # Only one constraint set is registered today, so reduce() over the single
        # resolved plan returns it unchanged (no __add__ call) - the write happens
        # directly on this plan's .df.
        resolved_plan = MagicMock()

        constraint_set_instance = MagicMock()
        constraint_set_instance.resolve = MagicMock(return_value=resolved_plan)
        multi_susie_constraint_set.return_value = constraint_set_instance

        FineMappingPlanGeneratorStep(
            session, input_path=input_path, output_path=output_path
        )

        # Study index read with the correct path.
        study_index.from_parquet.assert_called_once_with(session, input_path)

        # MultiSuSiE constraint set built with the expected fixed configuration.
        multi_susie_constraint_set.assert_called_once()
        _, kwargs = multi_susie_constraint_set.call_args
        assert set(kwargs["allowed_ancestries"]) == {
            LDPopulation.NFE,
            LDPopulation.AFR,
            LDPopulation.CSA,
        }
        assert set(kwargs["disallowed_flags"]) == {
            StudyAnalysisFlag.MULTIVARIATE_ANALYSIS,
            StudyAnalysisFlag.EXWAS,
            StudyAnalysisFlag.NON_ADDITIVE,
            StudyAnalysisFlag.WGS_WAS,
            StudyAnalysisFlag.GXG,
            StudyAnalysisFlag.GxE,
            StudyAnalysisFlag.CASE_CASE_STUDY,
        }
        assert set(kwargs["disallowed_reasons"]) == {
            StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
            StudyQualityCheck.FAILED_MEAN_BETA_CHECK,
            StudyQualityCheck.FAILED_PZ_CHECK,
            StudyQualityCheck.NO_OT_CURATION,
            StudyQualityCheck.SMALL_NUMBER_OF_SNPS,
        }
        assert kwargs["relative_sample_size_threshold"] == 0.95

        # resolve() called with the study index.
        constraint_set_instance.resolve.assert_called_once_with(study_index)

        # The resolved plan is written: coalesced to a single file, sorted, correct write
        # mode, partitioned by route, to output_path.
        resolved_plan.df.coalesce.assert_called_once_with(1)
        resolved_plan.df.coalesce.return_value.sortWithinPartitions.assert_called_once_with(
            "route", "runId", "studyId"
        )
        write_chain = (
            resolved_plan.df.coalesce.return_value.sortWithinPartitions.return_value
        )
        write_chain.write.mode.assert_called_once_with(session.write_mode)
        write_chain.write.mode.return_value.partitionBy.assert_called_once_with("route")
        write_chain.write.mode.return_value.partitionBy.return_value.parquet.assert_called_once_with(
            output_path
        )
