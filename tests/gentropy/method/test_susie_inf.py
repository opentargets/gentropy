"""Test of main SuSiE-inf functions."""

from __future__ import annotations

import numpy as np
import pyspark.sql.functions as f

from gentropy.common.session import Session
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.method.susie_inf import SUSIE_inf
from gentropy.susie_finemapper import SusieFineMapperStep


def _strong_qtl_locus(
    p: int = 400, zmax: float = 120.0, seed: int = 0
) -> tuple[np.ndarray, np.ndarray]:
    """Build a locus whose lead z-score exceeds sqrt(n) at eQTL sample size.

    At n = 10,725 a variant with |z| > 104 has z^2 > n, which is where the
    method-of-moments estimate of sigma^2 stops being identifiable.

    Args:
        p (int): number of variants in the locus
        zmax (float): effect size of the single causal variant, in z units
        seed (int): seed for the noise added to the marginal z-scores

    Returns:
        tuple[np.ndarray, np.ndarray]: z-scores and the LD matrix they were generated under
    """
    rng = np.random.default_rng(seed)
    idx = np.arange(p)
    ld = 0.85 ** (np.abs(idx[:, None] - idx[None, :]) * 3.0 / 25)
    np.fill_diagonal(ld, 1.0)
    b = np.zeros(p)
    b[p // 4] = zmax
    return ld.dot(b) + rng.normal(0, 0.5, p), ld


class TestSusieInfResidualVariance:
    """sigma^2 must stay identifiable, or the logBFs it scales are unusable.

    lbf_variable is approximately z^2 / (2 * sigmasq), so a sigma^2 driven to zero
    inflates every log-Bayes-factor without bound, and a negative one makes the whole
    locus NaN. Regression tests for the BigBrain eQTL fine-mapping, where logBF reached
    1.5e9 against a BrainSeq maximum of 567.
    """

    EQTL_N = 10_725
    """BigBrain sample size."""

    def test_no_nan_at_qtl_sample_size(self: TestSusieInfResidualVariance) -> None:
        """A strong cis-eQTL must not drive sigma^2 negative and NaN out the locus."""
        z, ld = _strong_qtl_locus()
        out = SUSIE_inf.susie_inf(z=z, LD=ld, L=10, n=self.EQTL_N, est_tausq=False)

        assert np.isfinite(out["sigmasq"]), "sigma^2 is not finite"
        assert out["sigmasq"] > 0, "sigma^2 is not positive"
        assert np.isfinite(out["lbf_variable"]).all(), "lbf_variable contains NaN"
        assert np.isfinite(out["PIP"]).all(), "PIP contains NaN"

    def test_bound_is_reported_when_it_binds(
        self: TestSusieInfResidualVariance,
    ) -> None:
        """Hitting the bound means the logBFs are inflated, so it must not pass silently."""
        z, ld = _strong_qtl_locus()
        out = SUSIE_inf.susie_inf(
            z=z, LD=ld, L=10, n=self.EQTL_N, est_tausq=False, sigmasq_min_fraction=0.01
        )

        assert out["sigmasq_floored"] is True, "bound bound silently"
        assert out["sigmasq"] == 0.01, "sigma^2 was not clipped to the bound"

    def test_sigmasq_never_exceeds_trait_variance(
        self: TestSusieInfResidualVariance,
    ) -> None:
        """sigma^2 is a residual variance, so it cannot exceed meansq."""
        z, ld = _strong_qtl_locus(zmax=2.0)
        meansq = 1.0
        out = SUSIE_inf.susie_inf(
            z=z, LD=ld, L=10, n=self.EQTL_N, meansq=meansq, est_tausq=False
        )

        assert out["sigmasq"] <= meansq, "sigma^2 exceeds the total trait variance"

    def test_est_sigmasq_false_recovers_calibrated_lbf(
        self: TestSusieInfResidualVariance,
    ) -> None:
        """Holding sigma^2 at meansq gives back logBF ~ z^2/2, the usable scale."""
        z, ld = _strong_qtl_locus()
        out = SUSIE_inf.susie_inf(
            z=z, LD=ld, L=10, n=self.EQTL_N, est_tausq=False, est_sigmasq=False
        )

        expected = float(np.max(np.abs(z)) ** 2 / 2)
        assert out["sigmasq_floored"] is False, (
            "bound applied although sigma^2 is fixed"
        )
        assert np.isclose(np.max(out["lbf_variable"]), expected, rtol=0.02), (
            "logBF is not on the z^2/2 scale"
        )

    def test_gwas_sample_size_is_unaffected(
        self: TestSusieInfResidualVariance,
    ) -> None:
        """At GWAS sample sizes z^2/n stays small, so the bound must never bind."""
        z, ld = _strong_qtl_locus()
        out = SUSIE_inf.susie_inf(z=z, LD=ld, L=10, n=500_000, est_tausq=False)

        assert out["sigmasq_floored"] is False, "bound bound in the GWAS regime"
        assert out["sigmasq"] > 0.9, "sigma^2 collapsed at GWAS sample size"


class TestSUSIE_inf:
    """Test of SuSiE-inf main functions."""

    def test_SUSIE_inf_lbf_moments(
        self: TestSUSIE_inf, sample_data_for_susie_inf: list[np.ndarray]
    ) -> None:
        """Test of SuSiE-inf LBF method of moments."""
        ld = sample_data_for_susie_inf[0]
        z = sample_data_for_susie_inf[1]
        lbf_moments = sample_data_for_susie_inf[2]
        susie_output = SUSIE_inf.susie_inf(z=z, LD=ld, est_tausq=True, method="moments")
        lbf_calc = susie_output["lbf_variable"][:, 0]
        assert np.allclose(lbf_calc, lbf_moments), (
            "LBFs for method of moments are not equal"
        )

    def test_SUSIE_inf_lbf_mle(
        self: TestSUSIE_inf, sample_data_for_susie_inf: list[np.ndarray]
    ) -> None:
        """Test of SuSiE-inf LBF maximum likelihood estimation."""
        ld = sample_data_for_susie_inf[0]
        z = sample_data_for_susie_inf[1]
        lbf_mle = sample_data_for_susie_inf[3]
        susie_output = SUSIE_inf.susie_inf(z=z, LD=ld, est_tausq=True, method="MLE")
        lbf_calc = susie_output["lbf_variable"][:, 0]
        assert np.allclose(lbf_calc, lbf_mle, atol=1e-1), (
            "LBFs for maximum likelihood estimation are not equal"
        )

    def test_SUSIE_inf_cred(
        self: TestSUSIE_inf, sample_data_for_susie_inf: list[np.ndarray]
    ) -> None:
        """Test of SuSiE-inf credible set generator."""
        ld = sample_data_for_susie_inf[0]
        z = sample_data_for_susie_inf[1]
        susie_output = SUSIE_inf.susie_inf(
            z=z,
            LD=ld,
            est_tausq=True,
        )
        cred = SUSIE_inf.cred_inf(susie_output["PIP"], LD=ld)
        assert cred[0] == [5]

    def test_SUSIE_inf_convert_to_study_locus(
        self: TestSUSIE_inf,
        sample_data_for_susie_inf: list[np.ndarray],
        sample_summary_statistics: SummaryStatistics,
        session: Session,
    ) -> None:
        """Test of SuSiE-inf credible set generator."""
        ld = sample_data_for_susie_inf[0]
        z = sample_data_for_susie_inf[1]
        susie_output = SUSIE_inf.susie_inf(
            z=z,
            LD=ld,
            est_tausq=False,
        )
        gwas_df = sample_summary_statistics._df.withColumn(
            "z", f.col("beta") / f.col("standardError")
        ).filter(f.col("z").isNotNull())
        gwas_df = gwas_df.limit(21)

        L1 = SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId="sample_id",
            region="sample_region",
            variant_index=gwas_df,
            cs_lbf_thr=2,
            ld_matrix=ld,
            lead_pval_threshold=1,
            purity_mean_r2_threshold=0,
            purity_min_r2_threshold=0,
            sum_pips=0.99,
            ld_min_r2=1,
            locusStart=1,
            locusEnd=2,
        )
        assert isinstance(L1, StudyLocus), "L1 is not an instance of StudyLocus"

    def test_SUSIE_inf_convert_to_study_locus_writes_study_type(
        self: TestSUSIE_inf,
        sample_data_for_susie_inf: list[np.ndarray],
        sample_summary_statistics: SummaryStatistics,
        session: Session,
    ) -> None:
        """Study type must reach the credible sets, or colocalisation drops them silently.

        StudyLocus.find_overlaps opens with filter(col("studyType").isNotNull()), so a
        null study type makes ColocalisationStep return an empty result rather than an
        error. This is what made the BigBrain eQTL credible sets unusable downstream.
        """
        ld = sample_data_for_susie_inf[0]
        z = sample_data_for_susie_inf[1]
        susie_output = SUSIE_inf.susie_inf(z=z, LD=ld, est_tausq=False)
        gwas_df = (
            sample_summary_statistics._df.withColumn(
                "z", f.col("beta") / f.col("standardError")
            )
            .filter(f.col("z").isNotNull())
            .limit(21)
        )

        study_locus = SusieFineMapperStep.susie_inf_to_studylocus(
            susie_output=susie_output,
            session=session,
            studyId="sample_id",
            region="sample_region",
            variant_index=gwas_df,
            cs_lbf_thr=2,
            ld_matrix=ld,
            lead_pval_threshold=1,
            purity_mean_r2_threshold=0,
            purity_min_r2_threshold=0,
            sum_pips=0.99,
            ld_min_r2=1,
            locusStart=1,
            locusEnd=2,
            studyType="eqtl",
        )

        assert study_locus is not None, "no credible sets were produced"
        study_types = [
            row["studyType"] for row in study_locus.df.select("studyType").collect()
        ]
        assert study_types, "no credible sets were produced"
        assert set(study_types) == {"eqtl"}, (
            f"studyType did not reach the credible sets: {set(study_types)}"
        )
