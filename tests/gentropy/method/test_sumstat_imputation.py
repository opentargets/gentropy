"""Test of sumstat imputation functions."""

from __future__ import annotations

import numpy as np

from gentropy.method.sumstat_imputation import SummaryStatisticsImputation


class TestSSImp:
    """Test of RAISS sumstat imputation main function."""

    def test_sumstat_imputation(
        self: TestSSImp, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of RAISS."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]

        unknowns = [5]
        known = [index for index in list(range(21)) if index not in unknowns]
        sig_t = ld[known, :][:, known]
        sig_i_t = ld[unknowns, :][:, known]
        zt = z[known]

        _l = SummaryStatisticsImputation.raiss_model(
            zt, sig_t, sig_i_t, lamb=0.01, rtol=0.01
        )
        assert (
            np.round(_l["imputation_r2"][0], decimals=4) == 0.9304
            and np.round(_l["mu"][0], decimals=4) == 9.7215
        )
