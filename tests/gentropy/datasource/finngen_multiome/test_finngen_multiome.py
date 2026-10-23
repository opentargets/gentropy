"""Tests for the FinnGen multiome single-cell eQTL ingestion."""

from __future__ import annotations

from pathlib import Path

import pyspark.sql.functions as f
import pytest

from gentropy.common.session import Session
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.finngen_multiome.finemapping import FinnGenMultiomeFinemapping
from gentropy.datasource.finngen_multiome.study_index import FinnGenMultiomeStudyIndex
from gentropy.finngen_multiome_ingestion import FinnGenMultiomeIngestionStep

DATA = "tests/gentropy/data_samples/finngen_multiome"
SNP_FILES = f"{DATA}/*.SUSIE.snp.tsv.gz"
CS_SUMMARY_FILES = f"{DATA}/*.SUSIE.cred.tsv.gz"
NOMINAL_FILES = f"{DATA}/*.cis_nominal.tsv.gz"
TEMPLATE = "gs://bucket/finngen_multiome_v1.eQTL.{cell_type}.cis_nominal.tsv.gz"
PREFIX = "FINNGEN_MULTIOME_V1"


@pytest.fixture()
def finemapping(session: Session) -> tuple[CredibleSet, CredibleSetVariant]:
    """Credible sets and their variants from the sample files."""
    return FinnGenMultiomeFinemapping.from_source(
        session=session,
        snp_files=SNP_FILES,
        cs_summary_files=CS_SUMMARY_FILES,
        nominal_files=NOMINAL_FILES,
        project_prefix=PREFIX,
    )


class TestFinnGenMultiomeFinemapping:
    """Credible sets built from the sample files."""

    def test_return_type(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Result is the credible sets and their variants."""
        credible_sets, variants = finemapping
        assert isinstance(credible_sets, CredibleSet)
        assert isinstance(variants, CredibleSetVariant)

    def test_filters(self, finemapping: tuple[CredibleSet, CredibleSetVariant]) -> None:
        """Only credible sets passing the Bayes factor and purity filters are kept.

        ENSG00000015475 credible set 2 fails both filters, ENSG00000025708 credible set 1
        fails purity (min r2 0.129).
        """
        credible_sets, _ = finemapping
        assert {r.studyId for r in credible_sets.df.collect()} == {
            f"{PREFIX}_ge_l1_B_ENSG00000015475",
            f"{PREFIX}_ge_l1_B_ENSG00000025770",
        }

    def test_members_match_cs_size(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Every credible set variant is kept (cs_size 1 and 14)."""
        credible_sets, variants = finemapping
        sizes = {
            r.studyId: r["count"]
            for r in variants.df.groupBy("credibleSetId")
            .count()
            .join(
                credible_sets.df.selectExpr("id AS credibleSetId", "studyId"),
                on="credibleSetId",
            )
            .collect()
        }
        assert sizes == {
            f"{PREFIX}_ge_l1_B_ENSG00000015475": 1,
            f"{PREFIX}_ge_l1_B_ENSG00000025770": 14,
        }

    def test_credible_set(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Locus spans all variants tested for the gene, not just the credible set."""
        credible_sets, _ = finemapping
        row = credible_sets.df.filter(
            f.col("studyId") == f"{PREFIX}_ge_l1_B_ENSG00000025770"
        ).first()
        assert row is not None
        assert (row.chromosome, row.locusStart, row.locusEnd) == (
            "22",
            49508306,
            50804129,
        )
        assert (row.studyType, row.fineMappingMethod) == ("sceqtl", "SuSiE")
        assert row.leadVariantId == "22_50570023_G_A"
        # Region-wide: over every variant tested for the gene.
        assert row.logBayesFactor == pytest.approx(21.7516922)
        assert row.purityMinR2 == pytest.approx(0.999156786305632)

    def test_variant_statistics(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Members carry the marginal statistics and those of their single effect."""
        _, variants = finemapping
        lead = variants.df.filter(f.col("variantId") == "22_50570023_G_A").first()
        assert lead is not None
        assert (lead.pValueMantissa, lead.pValueExponent) == (pytest.approx(1.47), -11)
        assert lead.reportedBeta == pytest.approx(0.34997)
        assert lead.reportedStandardError == pytest.approx(0.050592)
        assert lead.posteriorProbability == pytest.approx(0.0710690617602771)
        assert lead.logBayesFactor == pytest.approx(22.73197)
        # mean1 / alpha1 and sqrt((sd1^2 + mean1^2) / alpha1 - (mean1 / alpha1)^2) of the source:
        alpha, mean, sd = 0.0710690617602771, 0.0256293302217693, 0.0936637914943973
        assert lead.conditionalBeta == pytest.approx(mean / alpha)
        assert lead.conditionalStandardError == pytest.approx(
            ((sd**2 + mean**2) / alpha - (mean / alpha) ** 2) ** 0.5
        )
        assert lead.r2Overall == pytest.approx(0.8903827)

    def test_effect_allele_frequency(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Effect allele frequency comes from the nominal files for every variant."""
        _, variants = finemapping
        rows = variants.df.collect()
        assert all(r.effectAlleleFrequency is not None for r in rows)
        assert all(
            r.qualityControls == [CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value]
            for r in rows
        )

    def test_posterior_probabilities(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Posterior probabilities of each 95% credible set sum to at least 0.95."""
        _, variants = finemapping
        sums = (
            variants.df.groupBy("credibleSetId")
            .agg(f.sum("posteriorProbability").alias("s"))
            .collect()
        )
        assert all(0.95 <= r.s <= 1.0 + 1e-6 for r in sums)

    def test_variant_id_format(
        self, finemapping: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Variant identifiers are chromosome_position_ref_alt without a chr prefix."""
        _, variants = finemapping
        assert (
            variants.df.filter(
                ~f.col("variantId").rlike(r"^(\d+|X)_\d+_[ACGT]+_[ACGT]+$")
            ).count()
            == 0
        )

    def test_sub_significant_leads_are_dropped(self, session: Session) -> None:
        """The lead p-value threshold drops the credible set with its variants."""
        credible_sets, variants = FinnGenMultiomeFinemapping.from_source(
            session=session,
            snp_files=SNP_FILES,
            cs_summary_files=CS_SUMMARY_FILES,
            nominal_files=NOMINAL_FILES,
            project_prefix=PREFIX,
            lead_pvalue_threshold=1e-30,
        )
        assert [credible_sets.df.count(), variants.df.count()] == [1, 1]


def test_study_index(session: Session) -> None:
    """One study per cell type and gene with a credible set passing the filters."""
    study_index = FinnGenMultiomeStudyIndex.from_source(
        session=session,
        cs_summary_files=CS_SUMMARY_FILES,
        project_prefix=PREFIX,
        summary_stats_location_template=TEMPLATE,
    )
    assert isinstance(study_index, StudyIndex)
    rows = {r.studyId: r for r in study_index.df.collect()}
    assert set(rows) == {
        f"{PREFIX}_ge_l1_B_ENSG00000015475",
        f"{PREFIX}_ge_l1_B_ENSG00000025770",
    }
    row = rows[f"{PREFIX}_ge_l1_B_ENSG00000025770"]
    assert row.studyType == "sceqtl"
    assert row.geneId == "ENSG00000025770"
    assert row.biosampleFromSourceId == "CL_0000236"
    assert row.nSamples == 1103
    assert row.ldPopulationStructure[0].ldPopulation == "fin"
    assert (
        row.summarystatsLocation
        == "gs://bucket/finngen_multiome_v1.eQTL.l1.B.cis_nominal.tsv.gz"
    )


@pytest.mark.step_test
def test_finngen_multiome_ingestion_step(session: Session, tmp_path: Path) -> None:
    """Step writes the study index, credible sets and their variants."""
    FinnGenMultiomeIngestionStep(
        session=session,
        snp_files=SNP_FILES,
        cs_summary_files=CS_SUMMARY_FILES,
        nominal_files=NOMINAL_FILES,
        summary_stats_location_template=TEMPLATE,
        study_index_output_path=str(tmp_path / "study_index"),
        credible_set_output_path=str(tmp_path / "credible_set"),
        credible_set_variant_output_path=str(tmp_path / "credible_set_variant"),
        project_prefix=PREFIX,
        lead_pvalue_threshold=1e-3,
        credset_lbf_threshold=0.8685889638065036,
        purity_min_r2_threshold=0.25,
    )
    assert session.spark.read.parquet(str(tmp_path / "study_index")).count() == 2
    credible_sets = CredibleSet.from_parquet(session, str(tmp_path / "credible_set"))
    variants = CredibleSetVariant.from_parquet(
        session, str(tmp_path / "credible_set_variant")
    )
    assert [credible_sets.df.count(), variants.df.count()] == [2, 15]
