"""Test summary statistics quality control generation step."""

from pathlib import Path

import pytest

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.sumstat_qc_step import SummaryStatisticsQCStep


@pytest.mark.step_test
class TestSummaryStatisticsQCStep:
    """Test summary statistics quality control generation step."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session, tmp_path: Path) -> None:
        """Setup for the test."""
        self.pval_threshold = 1e-8
        self.n_partitions = 1
        self.output_path = tmp_path / "qc.parquet"

        sumstat_data = [
            (
                "S1",
                "10_73856419_C_A",
                10,
                73856419,
                0.5,
                1,
                3.1324,
                -650,
                None,
                0.4671,
            ),
            (
                "S1",
                "14_98074714_G_C",
                14,
                98074714,
                0.5,
                2,
                5.4275,
                -2890,
                None,
                0.4671,
            ),
        ]

        self.sumstat_path = str(tmp_path / "sumstat.parquet")

        session.spark.createDataFrame(
            sumstat_data, schema=SummaryStatistics.get_schema()
        ).write.parquet(self.sumstat_path)

    def test_summary_statistics_qc_step(self, session: Session) -> None:
        """Test summary statistics quality control generation step."""
        SummaryStatisticsQCStep(
            session=session,
            gwas_path=self.sumstat_path,
            output_path=str(self.output_path),
            pval_threshold=self.pval_threshold,
        )

        qc_df = session.spark.read.parquet(str(self.output_path))
        assert qc_df.count() == 1
        assert qc_df.schema == SummaryStatisticsQC.get_schema()
