"""Test of the qc of summary statistics."""
from __future__ import annotations

import pyspark.sql.functions as f
from gentropy.dataset.summary_statistics import SummaryStatistics


def test_qc_functions(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test all sumstat qc functions."""
    gwas = sample_summary_statistics.sanity_filter()

    assert gwas.number_of_snps() == (1663, 29)
    assert gwas.gc_lambda_check(lambda_threshold=2) == (True, 1.9159734429793385)
    assert gwas.sumstat_qc_beta_check() == (True, 0.001310962803607386)
    assert gwas.sumstat_n_eff_check(n_total=100000) == (True, 0)
    assert gwas.sumstat_qc_pz_check() == (True, 1.0, -1.0014145068737434e-10)


def test_neff_check_eaf(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test N_eff check using mock EAFs."""
    gwas = sample_summary_statistics.sanity_filter()
    gwas_df = gwas._df
    gwas_df = gwas_df.withColumn("effectAlleleFrequencyFromSource", f.lit(0.5))
    gwas._df = gwas_df

    assert gwas.sumstat_n_eff_check(n_total=100000) == (True, 0.5585536469698384)
