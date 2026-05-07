"""Tests for the L2G benchmark build step."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as f

from gentropy.common.session import Session
from gentropy.dataset.l2g_benchmark import L2GBenchmark
from gentropy.dataset.l2g_benchmark_build_report import L2GBenchmarkBuildReport
from gentropy.datasource.open_targets.l2g_gold_standard import (
    OpenTargetsL2GGoldStandard,
)
from gentropy.l2g_benchmark import L2GBenchmarkStep


@pytest.mark.step_test
class TestL2GBenchmarkStep:
    """Test the seed-only L2G benchmark step."""

    @pytest.fixture(autouse=True)
    def _setup(
        self,
        sample_l2g_gold_standard,
        tmp_path: Path,
    ) -> None:
        """Persist raw OTG curation input for the step."""
        self.input_path = str(tmp_path / "gold_standard_input")
        self.benchmark_path = str(tmp_path / "benchmark")
        self.build_report_path = str(tmp_path / "benchmark_build_report")
        self.benchmark_version = "ot_26_03_v1"
        self.release_version = "26.03"
        self.pipeline_run_id = "run-001"
        self.expected_seed_count = OpenTargetsL2GGoldStandard.parse_positive_curation(
            sample_l2g_gold_standard
        ).count()
        sample_l2g_gold_standard.write.mode("overwrite").json(self.input_path)

    def test_step(self, session: Session) -> None:
        """Step should emit the main benchmark artifact and build report."""
        assert not Path(self.benchmark_path).exists()
        assert not Path(self.build_report_path).exists()

        L2GBenchmarkStep(
            session=session,
            gold_standard_curation_path=self.input_path,
            benchmark_path=self.benchmark_path,
            build_report_path=self.build_report_path,
            benchmark_version=self.benchmark_version,
            release_version=self.release_version,
            pipeline_run_id=self.pipeline_run_id,
        )

        assert Path(self.benchmark_path).exists()
        assert Path(self.build_report_path).exists()

        benchmark = L2GBenchmark.from_parquet(session, self.benchmark_path)
        build_report = L2GBenchmarkBuildReport.from_parquet(
            session, self.build_report_path
        )
        report_row = build_report.df.collect()[0]

        assert benchmark.df.count() == self.expected_seed_count
        assert benchmark.df.select("studyLocusId", "geneId").distinct().count() == (
            benchmark.df.count()
        )
        assert benchmark.df.filter(f.col("goldStandardSet") != "positive").count() == 0
        assert (
            benchmark.df.select("benchmarkVersion").distinct().collect()[0][0]
            == self.benchmark_version
        )
        assert report_row["rowCount"] == self.expected_seed_count
        assert report_row["seedPositiveCount"] == self.expected_seed_count
        assert report_row["trainingIncludedCount"] == self.expected_seed_count
