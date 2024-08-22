"""Test of main CARMA functions."""

from __future__ import annotations

import numpy as np

from gentropy.method.carma import CARMA


class TestCARMA:
    """Test of CARMA main functions."""

    def test_CARMA_spike_slab_noEM_pips(
        self: TestCARMA, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of CARMA PIPs."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]
        pips = sample_data_for_carma[2]
        _l = CARMA.CARMA_spike_slab_noEM(z=z, ld=ld)
        assert np.allclose(np.round(np.corrcoef(_l["PIPs"], pips)[0, 1], decimals=2), 1)

    def test_CARMA_spike_slab_noEM_outliers(
        self: TestCARMA, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of CARMA outliers detection."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]

        _l = CARMA.CARMA_spike_slab_noEM(z=z, ld=ld)
        assert np.allclose(_l["Outliers"], 5)

    def test_MCS_modified(
        self: TestCARMA, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of MCS_modified and PIP_func functions on PIPs estiamtion."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]
        pips = sample_data_for_carma[2]

        l1 = CARMA._MCS_modified(
            z=z,
            ld_matrix=ld,
            outlier_BF_index=1 / 3.2,
            input_conditional_S_list=None,
            lambda_val=1,
            epsilon=1e-5 * 21,
            outlier_switch=True,
            tau=0.04,
        )
        l1_pips = CARMA._PIP_func(
            likeli=l1["B_list"]["set_gamma_margin"],
            model_space=l1["B_list"]["matrix_gamma"],
            p=21,
            num_causal=10,
        )
        assert np.allclose(np.round(np.corrcoef(l1_pips, pips)[0, 1], decimals=2), 1)

    def test_time_limited_CARMA_spike_slab_noEM_pips_no_restriction(
        self: TestCARMA, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of CARMA PIPs with liberal (no) time restriction."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]
        pips = sample_data_for_carma[2]
        _l = CARMA.time_limited_CARMA_spike_slab_noEM(z=z, ld=ld, sec_threshold=600)
        assert np.allclose(np.round(np.corrcoef(_l["PIPs"], pips)[0, 1], decimals=2), 1)

    def test_time_limited_CARMA_spike_slab_noEM_pips_restriction(
        self: TestCARMA, sample_data_for_carma: list[np.ndarray]
    ) -> None:
        """Test of CARMA PIPs with time restriction."""
        ld = sample_data_for_carma[0]
        z = sample_data_for_carma[1]
        _l = CARMA.time_limited_CARMA_spike_slab_noEM(z=z, ld=ld, sec_threshold=0.001)
        assert _l["Outliers"] is None and _l["PIPs"] is None
