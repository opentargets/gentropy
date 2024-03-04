"""Test of the qc of summary statistics."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
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


def test_pz_check(
    sample_summary_statistics: SummaryStatistics,
) -> None:
    """Test pz check."""
    gwas = sample_summary_statistics.sanity_filter()
    assert gwas.sumstat_qc_pz_check()[1] == 1.0
