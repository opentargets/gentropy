"""Tests for the SLE-map single-cell eQTL ingestion."""

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
from gentropy.datasource.sle_map.finemapping import SleMapFinemapping
from gentropy.datasource.sle_map.study_index import SleMapStudyIndex
from gentropy.sle_map_ingestion import SleMapIngestionStep

# 4 cell types and genes:
# - Classical_Monocytes ENSG00000213625: credible set 1 kept, credible set 2 with a lead p-value of 0.0016;
# - Allcells ENSG00000180902: credible set 1 kept, credible set 2 with a lead p-value of 0.02;
# - Classical_Monocytes ENSG00000100902: 1 credible set with a minimum r2 of 0.17;
# - CM_CD8_T_cells ENSG00000105193: no credible set.
DATA = "tests/gentropy/data_samples/sle_map"
MEMBERSHIPS = f"{DATA}/all_memberships.tsv"
COMPONENTS = f"{DATA}/all_components.tsv"
DIAGNOSTICS = f"{DATA}/all_diagnostics.tsv"


@pytest.fixture()
def sle_map(session: Session) -> tuple[CredibleSet, CredibleSetVariant]:
    """Credible sets with the default filters."""
    return SleMapFinemapping.from_source(session, MEMBERSHIPS, COMPONENTS, DIAGNOSTICS)


class TestCredibleSets:
    """SLE-map credible sets."""

    def test_filters(self, sle_map: tuple[CredibleSet, CredibleSetVariant]) -> None:
        """The impure credible set and the two with a weak lead are dropped."""
        credible_sets, variants = sle_map
        assert sorted(
            r.studyId for r in credible_sets.df.select("studyId").collect()
        ) == [
            "SLEmap_sceqtl_Allcells_ENSG00000180902",
            "SLEmap_sceqtl_Classical_Monocytes_ENSG00000213625",
        ]
        assert (
            variants.df.join(
                credible_sets.df.select(f.col("id").alias("credibleSetId")),
                on="credibleSetId",
                how="left_anti",
            ).count()
            == 0
        )

    def test_without_filters(self, session: Session) -> None:
        """Without filters, every credible set reported is kept."""
        credible_sets, variants = SleMapFinemapping.from_source(
            session,
            MEMBERSHIPS,
            COMPONENTS,
            DIAGNOSTICS,
            purity_min_r2_threshold=None,
            lead_pvalue_threshold=None,
        )
        assert credible_sets.df.count() == 5
        assert variants.df.count() == 137

    def test_posterior_probabilities(
        self, sle_map: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The members of each credible set hold at least 0.95 of its single effect."""
        _, variants = sle_map
        sums = [
            r.s
            for r in variants.df.groupBy("credibleSetId")
            .agg(f.sum("posteriorProbability").alias("s"))
            .collect()
        ]
        assert all(0.95 - 1e-6 <= s <= 1 + 1e-6 for s in sums)

    def test_credible_set_columns(
        self, sle_map: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The credible set takes the locus, purity and region Bayes factor of the component."""
        credible_sets, _ = sle_map
        row = credible_sets.df.filter(
            f.col("studyId") == "SLEmap_sceqtl_Allcells_ENSG00000180902"
        ).first()
        assert row is not None
        assert (row.chromosome, row.locusStart, row.locusEnd) == (
            "2",
            240735690,
            242147753,
        )
        assert row.logBayesFactor == pytest.approx(22.0010256060921)
        assert row.studyType == "sceqtl"
        assert row.fineMappingMethod == "SuSiE"

    def test_variant_columns(
        self, sle_map: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The member takes the marginal statistics of the alternative allele."""
        _, variants = sle_map
        rows = variants.df.collect()
        assert all(not r.variantId.startswith("chr") for r in rows)
        assert all(r.sampleSize in (211, 281) for r in rows)
        assert all(
            r.qualityControls == [CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value]
            for r in rows
        )
        assert all(
            r.conditionalBeta is not None and r.conditionalStandardError > 0
            for r in rows
        )
        assert all(r.reportedBeta is not None and r.pValueExponent < 0 for r in rows)


class TestStudyIndex:
    """SLE-map study index."""

    def test_studies(
        self, session: Session, sle_map: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """One study per cell type and gene with credible sets, with its biosample and sample size."""
        studies = SleMapStudyIndex.from_source(session, DIAGNOSTICS, sle_map[0])
        rows = {r.studyId: r for r in studies.df.collect()}
        assert set(rows) == {
            "SLEmap_sceqtl_Allcells_ENSG00000180902",
            "SLEmap_sceqtl_Classical_Monocytes_ENSG00000213625",
        }
        row = rows["SLEmap_sceqtl_Classical_Monocytes_ENSG00000213625"]
        assert (row.geneId, row.biosampleFromSourceId, row.nSamples) == (
            "ENSG00000213625",
            "CL_0000576",
            211,
        )
        assert "biosampleId" not in studies.df.columns
        assert sum(s.sampleSize for s in row.discoverySamples) == 211
        assert sum(p.relativeSampleSize for p in row.ldPopulationStructure) == (
            pytest.approx(1.0)
        )
        assert (row.projectId, row.studyType) == ("OTAR2064", "sceqtl")

    def test_split_by_ancestry(self) -> None:
        """Every sample size of the cohort is split into parts summing to it."""
        assert all(
            sum(size for size, _ in SleMapStudyIndex.split_by_ancestry(n)) == n
            for n in range(1, 282)
        )


@pytest.mark.step_test
def test_sle_map_ingestion_step(session: Session, tmp_path: Path) -> None:
    """Step writes the study index, credible sets and their variants."""
    SleMapIngestionStep(
        session=session,
        memberships_path=MEMBERSHIPS,
        components_path=COMPONENTS,
        diagnostics_path=DIAGNOSTICS,
        study_index_output_path=str(tmp_path / "study"),
        credible_set_output_path=str(tmp_path / "credible_set"),
        credible_set_variant_output_path=str(tmp_path / "credible_set_variant"),
    )
    studies = StudyIndex.from_parquet(session, str(tmp_path / "study"))
    credible_sets = CredibleSet.from_parquet(session, str(tmp_path / "credible_set"))
    variants = CredibleSetVariant.from_parquet(
        session, str(tmp_path / "credible_set_variant")
    )
    assert [studies.df.count(), credible_sets.df.count()] == [2, 2]
    assert variants.df.count() > 0
