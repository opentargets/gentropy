"""Tests for fine-mapping SuSiE results from FinnGen."""

from __future__ import annotations

from pathlib import Path

import pytest

from gentropy.common.session import Session
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping
from gentropy.finngen_finemapping_ingestion import FinnGenFinemappingIngestionStep

SNP_FILES = "tests/gentropy/data_samples/finngen_R9_AB1_EBV.SUSIE.snp.gz"
CS_SUMMARY_FILES = "tests/gentropy/data_samples/finngen_credset_summary_sample.tsv"
# Synthetic allele frequencies for three of the four members of the sample credible set:
ANNOTATED_VARIANTS = "tests/gentropy/data_samples/finngen_annotated_variants_sample.tsv"


def _finemapping(
    session: Session, lead_pvalue_threshold: float | None = None
) -> tuple[CredibleSet, CredibleSetVariant]:
    """Process the sample FinnGen fine-mapping files.

    Args:
        session (Session): Session object.
        lead_pvalue_threshold (float | None): Lead p-value threshold.

    Returns:
        tuple[CredibleSet, CredibleSetVariant]: The credible sets and their variants.
    """
    return FinnGenFinemapping.from_finngen_susie_finemapping(
        spark=session.spark,
        finngen_susie_finemapping_snp_files=SNP_FILES,
        finngen_susie_finemapping_cs_summary_files=CS_SUMMARY_FILES,
        finngen_annotated_variants_path=ANNOTATED_VARIANTS,
        finngen_release_prefix="FINNGEN_R11",
        lead_pvalue_threshold=lead_pvalue_threshold,
    )


def test_credible_set_carries_the_region_and_its_bayes_factor(
    session: Session,
) -> None:
    """The sample's one credible set is bounded by its region and led by the highest alpha."""
    credible_sets, _ = _finemapping(session)

    [credible_set] = credible_sets.df.collect()
    assert credible_set.studyId == "FINNGEN_R11_AB1_EBV"
    assert (
        credible_set.chromosome,
        credible_set.locusStart,
        credible_set.locusEnd,
    ) == ("6", 1412516, 4412516)
    assert credible_set.leadVariantId == "6_2912516_CCA_C"
    assert (credible_set.studyType, credible_set.fineMappingMethod) == (
        "gwas",
        "SuSiE",
    )
    # Region-wide: over all 89 variants of the region, not FinnGen's cs_log10bf of 4.46.
    assert credible_set.logBayesFactor == pytest.approx(15.8055011)
    assert credible_set.purityMeanR2 == pytest.approx(0.678175582701)
    assert credible_set.purityMinR2 == pytest.approx(0.468032488641)
    assert credible_set.confidence is None
    assert credible_set.qualityControls == []


def test_credible_set_variants_carry_marginal_and_single_effect_statistics(
    session: Session,
) -> None:
    """Members carry the marginal statistics and the statistics of their single effect."""
    credible_sets, variants = _finemapping(session)

    rows = {row.variantId: row for row in variants.df.collect()}
    assert set(rows) == {
        "6_2898066_G_T",
        "6_2898362_A_G",
        "6_2912043_G_A",
        "6_2912516_CCA_C",
    }
    assert {row.credibleSetId for row in rows.values()} == {credible_sets.df.first().id}

    lead = rows["6_2912516_CCA_C"]
    assert (lead.pValueMantissa, lead.pValueExponent) == (pytest.approx(4.909), -11)
    assert lead.reportedBeta == pytest.approx(0.196413)
    assert lead.reportedStandardError == pytest.approx(0.0298788)
    assert lead.posteriorProbability == pytest.approx(0.367106415849495)
    assert lead.logBayesFactor == pytest.approx(19.2932005)
    assert lead.conditionalBeta == pytest.approx(0.0705310811656966)
    assert lead.conditionalStandardError == pytest.approx(0.0943204169456985)
    assert lead.r2Overall == pytest.approx(1.0)
    assert lead.sampleSize is None
    assert lead.rescaledAbsBeta is None


def test_effect_allele_frequency_is_the_alternative_allele_frequency(
    session: Session,
) -> None:
    """The frequency comes from the annotated variants, not from the folded maf."""
    _, variants = _finemapping(session)

    rows = {row.variantId: row for row in variants.df.collect()}
    # The alternative allele is the major one here: maf is 0.379568, AF is 1 - maf.
    lead = rows["6_2912516_CCA_C"]
    assert lead.effectAlleleFrequency == pytest.approx(0.620432)
    assert lead.qualityControls == [
        CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value
    ]
    # Not in the annotated variants:
    missing = rows["6_2898362_A_G"]
    assert missing.effectAlleleFrequency is None
    assert missing.qualityControls == []


def test_credible_sets_with_a_sub_significant_lead_are_dropped(
    session: Session,
) -> None:
    """The lead p-value threshold drops the credible set with its variants."""
    assert [d.df.count() for d in _finemapping(session, 1e-5)] == [1, 4]
    assert [d.df.count() for d in _finemapping(session, 1e-12)] == [0, 0]


@pytest.mark.step_test
def test_finngen_finemapping_ingestion_step(session: Session, tmp_path: Path) -> None:
    """The step writes the credible sets and their variants."""
    credible_set_path = tmp_path / "credible_set"
    credible_set_variant_path = tmp_path / "credible_set_variant"
    FinnGenFinemappingIngestionStep(
        session=session,
        finngen_annotated_variants_path=ANNOTATED_VARIANTS,
        finngen_finemapping_out=str(credible_set_path),
        finngen_finemapping_credible_set_variant_out=str(credible_set_variant_path),
        finngen_susie_finemapping_cs_summary_files=CS_SUMMARY_FILES,
        finngen_susie_finemapping_snp_files=SNP_FILES,
        finngen_finemapping_lead_pvalue_threshold=1e-5,
        finngen_release_prefix="FINNGEN_R11",
    )

    credible_sets = CredibleSet.from_parquet(session, str(credible_set_path))
    variants = CredibleSetVariant.from_parquet(session, str(credible_set_variant_path))
    assert credible_sets.df.count() == 1
    assert variants.df.count() == 4
