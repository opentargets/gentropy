"""Test of the qc of summary statistics."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pyspark.sql.functions as f
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.sumstat_quality_controls import SummaryStatisticsQC


def test_qc_functions(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test all sumstat qc functions."""
    gwas = sample_summary_statistics.sanity_filter()
    QC = SummaryStatisticsQC.get_quality_control_metrics(
        gwas=gwas, limit=100000, min_count=100, n_total=100000
    )
    QC = QC.toPandas()

    assert gwas.number_of_snps() == (1663, 29)
    assert QC["n_variants"].iloc[0] == 1663
    assert QC["n_variants_sig"].iloc[0] == 29
    assert np.round(QC["gc_lambda"].iloc[0], 4) == 1.916
    assert np.round(QC["mean_beta"].iloc[0], 4) == 0.0013
    assert np.round(QC["pz_beta"].iloc[0], 6) == 1
    assert np.round(QC["pz_intercept"].iloc[0], 6) == 0
    assert pd.isna(QC["se_N"].iloc[0])


def test_neff_check_eaf(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test N_eff check using mock EAFs."""
    gwas = sample_summary_statistics.sanity_filter()
    gwas_df = gwas._df
    gwas_df = gwas_df.withColumn("effectAlleleFrequencyFromSource", f.lit(0.5))
    gwas._df = gwas_df

    QC = SummaryStatisticsQC.get_quality_control_metrics(
        gwas=gwas, limit=100000, min_count=100, n_total=100000
    )
    QC = QC.toPandas()
    assert np.round(QC["se_N"].iloc[0], 4) == 0.5586
