"""Tests for the eQTLGen single-cell eQTL and co-eQTL ingestion."""

from __future__ import annotations

from pathlib import Path

import pyspark.sql.functions as f
import pytest
from pyspark.sql import DataFrame

from gentropy.common.session import Session
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_index import StudyIndex
from gentropy.datasource.eqtlgen.finemapping import EqtlGenDataset, EqtlGenFinemapping
from gentropy.datasource.eqtlgen.study_index import EqtlGenStudyIndex
from gentropy.eqtlgen_ingestion import EqtlGenIngestionStep

# B cells, chromosome 22: the single-cell eQTLs of 3 genes and the co-eQTLs of 2 gene pairs.
DATA = "tests/gentropy/data_samples/eqtlgen"
SINGLE_CELL_FILES = f"{DATA}/finemap_*/*.txt"
COEQTL_FILES = f"{DATA}/joint_co_eQTL_fine_mapping_*/*.txt"
# SMDT1 (ENSG00000183172) and decoys named SMDT1 on another chromosome, away from the
# feature, and as an obsolete symbol.
TARGET = f"{DATA}/target.parquet"


@pytest.fixture()
def single_cell_variants(session: Session) -> DataFrame:
    """Every variant tested in the single-cell eQTL sample."""
    return EqtlGenFinemapping.read(
        session, EqtlGenDataset.SINGLE_CELL_EQTL, SINGLE_CELL_FILES
    )


@pytest.fixture()
def coeqtl_variants(session: Session) -> DataFrame:
    """Every variant tested in the co-eQTL sample."""
    return EqtlGenFinemapping.read(session, EqtlGenDataset.COEQTL, COEQTL_FILES)


@pytest.fixture()
def single_cell(
    single_cell_variants: DataFrame,
) -> tuple[CredibleSet, CredibleSetVariant]:
    """Single-cell eQTL credible sets with the default filters."""
    return EqtlGenFinemapping.from_source(single_cell_variants)


@pytest.fixture()
def coeqtl(coeqtl_variants: DataFrame) -> tuple[CredibleSet, CredibleSetVariant]:
    """Co-eQTL credible sets with the default filters."""
    return EqtlGenFinemapping.from_source(coeqtl_variants)


def _posterior_sums(variants: CredibleSetVariant) -> list[float]:
    return [
        r.s
        for r in variants.df.groupBy("credibleSetId")
        .agg(f.sum("posteriorProbability").alias("s"))
        .collect()
    ]


class TestRead:
    """The files are read into one row per variant and feature."""

    def test_single_cell_studies(self, single_cell_variants: DataFrame) -> None:
        """The study is the cell type of the folder and the gene."""
        assert single_cell_variants.count() == 814
        assert {
            (r.cellType, r.studyId)
            for r in single_cell_variants.select("cellType", "studyId")
            .distinct()
            .collect()
        } == {
            ("B", "eQTLGen_B_ENSG00000100038"),
            ("B", "eQTLGen_B_ENSG00000211679"),
            ("B", "eQTLGen_B_ENSG00000211685"),
        }

    def test_single_cell_effect_allele(self, single_cell_variants: DataFrame) -> None:
        """`beta` is of the reference allele, `reportedBeta` of the alternative allele."""
        row = single_cell_variants.filter(
            f.col("variantId") == "22_22934885_T_A"
        ).first()
        assert row is not None
        # beta of the source, for the assessed (reference) allele T: 0.328995977023599
        assert row.reportedBeta == pytest.approx(-0.328995977023599)
        assert row.reportedStandardError == pytest.approx(0.0489494041838994)
        assert row.sampleSize == 1962

    def test_coeqtl_studies(self, coeqtl_variants: DataFrame) -> None:
        """The study is the cell type and the gene pair; there is no effect size."""
        assert coeqtl_variants.count() == 880
        assert {
            r.studyId for r in coeqtl_variants.select("studyId").distinct().collect()
        } == {"eQTLGen_B_SMDT1_RPS17", "eQTLGen_B_SMDT1_RPS15A"}
        assert "reportedBeta" not in coeqtl_variants.columns

    def test_coeqtl_cell_type_of_t_cells(
        self, session: Session, tmp_path: Path
    ) -> None:
        """CD4 T cells are `CD4` in the co-eQTL study identifiers, as in earlier releases."""
        folder = tmp_path / "joint_co_eQTL_fine_mapping_CD4_T_SF_coloc_20250204"
        folder.mkdir()
        source = next(Path(DATA).glob("joint_co_eQTL_fine_mapping_*/*.txt"))
        (folder / source.name).write_text(source.read_text())
        row = EqtlGenFinemapping.read(
            session, EqtlGenDataset.COEQTL, str(folder / "*.txt")
        ).first()
        assert row is not None
        assert (row.cellType, row.studyId.split("_")[1]) == ("CD4_T", "CD4")


class TestSingleCellCredibleSets:
    """Single-cell eQTL credible sets."""

    def test_filters(self, single_cell: tuple[CredibleSet, CredibleSetVariant]) -> None:
        """ENSG00000100038 has no credible set; the credible set 1 of ENSG00000211685 has a lead p-value of 0.045."""
        credible_sets, _ = single_cell
        assert sorted(
            (r.studyId, r.leadVariantId) for r in credible_sets.df.collect()
        ) == [
            ("eQTLGen_B_ENSG00000211679", "22_22936414_C_T"),
            ("eQTLGen_B_ENSG00000211685", "22_22934849_GCCTGTGTGGAGGGGA_G"),
        ]

    def test_credible_set_effect(
        self, single_cell: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The credible set 2 of ENSG00000211685 is the SuSiE effect 1, not the effect 2."""
        credible_sets, variants = single_cell
        row = credible_sets.df.filter(
            f.col("studyId") == "eQTLGen_B_ENSG00000211685"
        ).first()
        assert row is not None
        assert row.logBayesFactor == pytest.approx(153.0052247132886)
        lead = variants.df.filter(f.col("credibleSetId") == row.id).orderBy(
            f.desc("posteriorProbability")
        )
        assert [
            (r.variantId, round(r.posteriorProbability, 4)) for r in lead.collect()
        ] == [
            ("22_22934849_GCCTGTGTGGAGGGGA_G", 0.605),
            ("22_22934255_C_T", 0.2132),
            ("22_22934885_T_A", 0.1818),
        ]

    def test_locus(self, single_cell: tuple[CredibleSet, CredibleSetVariant]) -> None:
        """The locus spans every variant tested for the gene."""
        credible_sets, _ = single_cell
        assert {
            (r.chromosome, r.locusStart, r.locusEnd, r.studyType, r.fineMappingMethod)
            for r in credible_sets.df.collect()
        } == {("22", 21926544, 23906525, "sceqtl", "SuSiE")}

    def test_posterior_probabilities(
        self, single_cell: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The posterior probabilities of each credible set sum to at least 0.95."""
        _, variants = single_cell
        assert all(0.95 <= s <= 1.0 + 1e-6 for s in _posterior_sums(variants))


class TestCoeqtlCredibleSets:
    """Co-eQTL credible sets."""

    def test_credible_sets(
        self, coeqtl: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """Every credible set passes the filters; a credible set can have 80 members."""
        credible_sets, variants = coeqtl
        assert credible_sets.df.count() == 3
        assert variants.df.count() == 165
        assert all(0.95 <= s <= 1.0 + 1e-6 for s in _posterior_sums(variants))

    def test_no_effect_size(
        self, coeqtl: tuple[CredibleSet, CredibleSetVariant]
    ) -> None:
        """The co-eQTL files give no effect size."""
        _, variants = coeqtl
        assert variants.df.filter(f.col("reportedBeta").isNotNull()).count() == 0

    def test_bayes_factor_filter(self, coeqtl_variants: DataFrame) -> None:
        """The credible set 2 of SMDT1 and RPS15A has a log10 Bayes factor of 1.65."""
        credible_sets, _ = EqtlGenFinemapping.from_source(
            coeqtl_variants, min_credible_set_log10_bayes_factor=2.0
        )
        assert credible_sets.df.count() == 2


class TestStudyIndex:
    """One study per feature with credible sets."""

    def test_single_cell(
        self,
        session: Session,
        single_cell_variants: DataFrame,
        single_cell: tuple[CredibleSet, CredibleSetVariant],
    ) -> None:
        """The gene is the feature; studies without credible sets are left out."""
        study_index = EqtlGenStudyIndex.from_source(
            session,
            EqtlGenDataset.SINGLE_CELL_EQTL,
            single_cell_variants,
            single_cell[0],
        )
        assert isinstance(study_index, StudyIndex)
        rows = {r.studyId: r for r in study_index.df.collect()}
        assert set(rows) == {"eQTLGen_B_ENSG00000211679", "eQTLGen_B_ENSG00000211685"}
        row = rows["eQTLGen_B_ENSG00000211685"]
        assert (row.geneId, row.traitFromSource, row.biosampleFromSourceId) == (
            "ENSG00000211685",
            "IGLC7",
            "CL_0000236",
        )
        assert (row.studyType, row.projectId, row.nSamples) == (
            "sceqtl",
            "eQTLGen",
            1962,
        )

    def test_coeqtl(
        self,
        session: Session,
        coeqtl_variants: DataFrame,
        coeqtl: tuple[CredibleSet, CredibleSetVariant],
    ) -> None:
        """EGene SMDT1 is the gene overlapping the feature; the sample size is the largest."""
        study_index = EqtlGenStudyIndex.from_source(
            session, EqtlGenDataset.COEQTL, coeqtl_variants, coeqtl[0], TARGET
        )
        rows = {r.studyId: r for r in study_index.df.collect()}
        assert {k: (r.geneId, r.nSamples) for k, r in rows.items()} == {
            "eQTLGen_B_SMDT1_RPS15A": ("ENSG00000183172", 1097),
            "eQTLGen_B_SMDT1_RPS17": ("ENSG00000183172", 1096),
        }
        assert rows["eQTLGen_B_SMDT1_RPS17"].traitFromSource == "SMDT1_RPS17"

    def test_coeqtl_needs_target_index(
        self,
        session: Session,
        coeqtl_variants: DataFrame,
        coeqtl: tuple[CredibleSet, CredibleSetVariant],
    ) -> None:
        """Co-eQTL gene symbols can not be resolved without a target index."""
        with pytest.raises(ValueError, match="target index"):
            EqtlGenStudyIndex.from_source(
                session, EqtlGenDataset.COEQTL, coeqtl_variants, coeqtl[0]
            )


@pytest.mark.step_test
def test_eqtlgen_ingestion_step(session: Session, tmp_path: Path) -> None:
    """Step writes the study index, credible sets and their variants of the co-eQTLs."""
    EqtlGenIngestionStep(
        session=session,
        dataset="coeqtl",
        finemapping_files=COEQTL_FILES,
        study_index_output_path=str(tmp_path / "study"),
        credible_set_output_path=str(tmp_path / "credible_set"),
        credible_set_variant_output_path=str(tmp_path / "credible_set_variant"),
        target_index_path=TARGET,
    )
    studies = StudyIndex.from_parquet(session, str(tmp_path / "study"))
    credible_sets = CredibleSet.from_parquet(session, str(tmp_path / "credible_set"))
    variants = CredibleSetVariant.from_parquet(
        session, str(tmp_path / "credible_set_variant")
    )
    assert [
        studies.df.count(),
        credible_sets.df.count(),
        variants.df.count(),
    ] == [2, 3, 165]
