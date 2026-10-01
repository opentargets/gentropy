"""Tests for the FinnGen multiome single-cell eQTL ingestion."""

from __future__ import annotations

from pathlib import Path

import pyspark.sql.functions as f
import pytest

from gentropy.common.session import Session
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
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
def credible_sets(session: Session) -> StudyLocus:
    """Credible sets from the sample files."""
    return FinnGenMultiomeFinemapping.from_source(
        session=session,
        snp_files=SNP_FILES,
        cs_summary_files=CS_SUMMARY_FILES,
        nominal_files=NOMINAL_FILES,
        project_prefix=PREFIX,
    )


class TestFinnGenMultiomeFinemapping:
    """Credible sets built from the sample files."""

    def test_return_type(self, credible_sets: StudyLocus) -> None:
        """Result is a StudyLocus."""
        assert isinstance(credible_sets, StudyLocus)

    def test_filters(self, credible_sets: StudyLocus) -> None:
        """Only credible sets passing the Bayes factor and purity filters are kept.

        ENSG00000015475 credible set 2 fails both filters, ENSG00000025708 credible set 1
        fails purity (min r2 0.129).
        """
        kept = {
            (r.studyId, r.credibleSetIndex)
            for r in credible_sets.df.select("studyId", "credibleSetIndex").collect()
        }
        assert kept == {
            (f"{PREFIX}_ge_l1_B_ENSG00000015475", 1),
            (f"{PREFIX}_ge_l1_B_ENSG00000025770", 1),
        }

    def test_locus_matches_cs_size(self, credible_sets: StudyLocus) -> None:
        """Locus holds every credible set variant (cs_size 1 and 14)."""
        sizes = {
            r.studyId: r.n
            for r in credible_sets.df.select(
                "studyId", f.size("locus").alias("n")
            ).collect()
        }
        assert sizes == {
            f"{PREFIX}_ge_l1_B_ENSG00000015475": 1,
            f"{PREFIX}_ge_l1_B_ENSG00000025770": 14,
        }

    def test_locus_boundaries(self, credible_sets: StudyLocus) -> None:
        """Locus boundaries span all variants tested for the gene, not just the credible set."""
        row = credible_sets.df.filter(
            f.col("studyId") == f"{PREFIX}_ge_l1_B_ENSG00000025770"
        ).first()
        assert row is not None
        assert (row.locusStart, row.locusEnd) == (49508306, 50804129)

    def test_effect_allele_frequency(self, credible_sets: StudyLocus) -> None:
        """Effect allele frequency comes from the nominal files for every lead variant."""
        assert (
            credible_sets.df.filter(
                f.col("effectAlleleFrequencyFromSource").isNull()
            ).count()
            == 0
        )

    def test_posterior_probabilities(self, credible_sets: StudyLocus) -> None:
        """Posterior probabilities of each 95% credible set sum to at least 0.95."""
        sums = credible_sets.df.select(
            f.aggregate(
                "locus", f.lit(0.0), lambda acc, x: acc + x.posteriorProbability
            ).alias("s")
        ).collect()
        assert all(0.95 <= r.s <= 1.0 + 1e-6 for r in sums)

    def test_variant_id_format(self, credible_sets: StudyLocus) -> None:
        """Variant identifiers are chromosome_position_ref_alt without a chr prefix."""
        assert (
            credible_sets.df.filter(
                ~f.col("variantId").rlike(r"^(\d+|X)_\d+_[ACGT]+_[ACGT]+$")
            ).count()
            == 0
        )


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
    """Step writes the study index and credible sets."""
    FinnGenMultiomeIngestionStep(
        session=session,
        snp_files=SNP_FILES,
        cs_summary_files=CS_SUMMARY_FILES,
        nominal_files=NOMINAL_FILES,
        summary_stats_location_template=TEMPLATE,
        study_index_output_path=str(tmp_path / "study_index"),
        credible_set_output_path=str(tmp_path / "credible_set"),
        project_prefix=PREFIX,
        lead_pvalue_threshold=1e-3,
        credset_lbf_threshold=0.8685889638065036,
        purity_min_r2_threshold=0.25,
    )
    assert session.spark.read.parquet(str(tmp_path / "study_index")).count() == 2
    assert session.spark.read.parquet(str(tmp_path / "credible_set")).count() == 2
