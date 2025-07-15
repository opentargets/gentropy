"""Test summary statistics qc dataset."""

from __future__ import annotations

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC


class TestSummaryStatisticsQC:
    """Test SummaryStatisticsQC constructor and methods."""

    MOCK_DATA = [
        ("S1", 0.45, 6.78, 8.47, 0.55, 2, 1),
        ("S2", 0.26, -2.15, 4.38, 0.04, 2, 0),
    ]

    def test_default_constructor(
        self: TestSummaryStatisticsQC,
        spark: SparkSession,
    ) -> None:
        """Test the default constructor."""
        df = spark.createDataFrame(
            data=self.MOCK_DATA, schema=SummaryStatisticsQC.get_schema()
        )
        sumstat_qc = SummaryStatisticsQC(_df=df)

        assert isinstance(sumstat_qc, SummaryStatisticsQC)
        assert sumstat_qc.df.count() == 2
        assert sumstat_qc.df.schema == SummaryStatisticsQC.get_schema()

    def test_constructor_from_summary_statistics(
        self: TestSummaryStatisticsQC,
        sample_summary_statistics: SummaryStatistics,
    ) -> None:
        """Test the constructor that creates QC dataset from sumstat."""
        gwas = sample_summary_statistics.sanity_filter()
        sumstat_qc = SummaryStatisticsQC.from_summary_statistics(
            gwas=gwas, pval_threshold=5e-8
        )

        assert isinstance(sumstat_qc, SummaryStatisticsQC)
        qc = sumstat_qc.df.toPandas()

        assert qc["n_variants"].iloc[0] == 1663
        assert qc["n_variants_sig"].iloc[0] == 29
        assert np.round(qc["gc_lambda"].iloc[0], 4) == 1.916
        assert np.round(qc["mean_beta"].iloc[0], 4) == 0.0013
        assert np.round(qc["mean_diff_pz"].iloc[0], 6) == 0
        assert np.round(qc["se_diff_pz"].iloc[0], 6) == 0

    def test_constructor_from_summary_statistics_multiple_sid(
        self: TestSummaryStatisticsQC,
        sample_summary_statistics: SummaryStatistics,
    ) -> None:
        """Test the constructor that creates QC dataset from sumstat with multiple studyId."""
        gwas = sample_summary_statistics.sanity_filter()
        gwas_df = gwas.df.withColumn(
            "studyId", f.when(f.rand() < 0.5, "new_value").otherwise(f.col("studyId"))
        )
        gwas._df = gwas_df

        sumstat_qc = SummaryStatisticsQC.from_summary_statistics(gwas=gwas)
        qc = sumstat_qc.df.toPandas()
        assert qc.shape == (2, 7)
