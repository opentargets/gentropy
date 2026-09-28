"""Tests for study index dataset from eQTL Catalogue."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from gentropy import StudyIndex, StudyLocus
from gentropy.common.session import Session
from gentropy.datasource.eqtl_catalogue.finemapping import EqtlCatalogueFinemapping
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex


@pytest.fixture
def processed_finemapping_df(
    sample_eqtl_catalogue_finemapping_credible_sets: DataFrame,
    sample_eqtl_catalogue_finemapping_lbf: DataFrame,
    sample_eqtl_catalogue_studies_metadata: DataFrame,
) -> DataFrame:
    """Fixture to provide a processed finemapping DataFrame."""
    return EqtlCatalogueFinemapping.parse_susie_results(
        sample_eqtl_catalogue_finemapping_credible_sets,
        sample_eqtl_catalogue_finemapping_lbf,
        sample_eqtl_catalogue_studies_metadata,
    )


class TestEqtlCatalogueStudyIndex:
    """Test the correctness of the study index dataset from eQTL Catalogue."""

    @pytest.fixture(autouse=True)
    def _setup(self: TestEqtlCatalogueStudyIndex, session: Session) -> None:
        self.session = session

    def test__read_studies_from_source(self: TestEqtlCatalogueStudyIndex) -> None:
        """Test reading studies from source."""
        data = EqtlCatalogueStudyIndex.read_studies_from_source(
            "tests/gentropy/data_samples/sample_eqtl_catalogue_studies.tsv",
            [],
            session=self.session,
        )
        assert isinstance(data, DataFrame)
        assert data.count() == 1
        assert set(data.columns) == {
            "study_id",
            "dataset_id",
            "study_label",
            "sample_group",
            "tissue_id",
            "tissue_label",
            "condition_label",
            "sample_size",
            "quant_method",
            "pmid",
            "study_type",
        }

    def test__read_studies_from_source_with_blacklist(
        self: TestEqtlCatalogueStudyIndex,
    ) -> None:
        """Test reading studies from source with blacklist."""
        data = EqtlCatalogueStudyIndex.read_studies_from_source(
            "tests/gentropy/data_samples/sample_eqtl_catalogue_studies.tsv",
            ["ge"],
            session=self.session,
        )
        assert isinstance(data, DataFrame)
        assert data.count() == 0

    def test__read_studies_from_source_with_invalid_quant_method(
        self: TestEqtlCatalogueStudyIndex,
    ) -> None:
        """Test reading studies from source with invalid quant method."""
        with pytest.raises(ValueError):
            EqtlCatalogueStudyIndex.read_studies_from_source(
                "tests/gentropy/data_samples/sample_eqtl_catalogue_studies.tsv",
                ["invalid_quant_method"],
                session=None,
            )

    def test__from_susie_results(
        self: TestEqtlCatalogueStudyIndex, processed_finemapping_df: DataFrame
    ) -> None:
        """Test creating study index from SuSIE results."""
        data = EqtlCatalogueStudyIndex.from_susie_results(processed_finemapping_df)
        assert isinstance(data, StudyIndex), "Expected StudyIndex instance"
        assert data.df.count() == 1, "Expected 1 study"


class TestEqtlCatalogueFinemapping:
    """Test the correctness of the study locus dataset from eQTL Catalogue."""

    @pytest.fixture(autouse=True)
    def _setup(self, session: Session) -> None:
        """Set up the test."""
        self.session = session

    def test__read_lbf_from_source(self: TestEqtlCatalogueFinemapping) -> None:
        """Test reading LBF from source."""
        data = EqtlCatalogueFinemapping.read_lbf_from_source(
            "tests/gentropy/data_samples/QTD000001/QTS00001/QTS000001.lbf_variable.parquet",
            session=self.session,
        )
        assert isinstance(data, DataFrame)
        assert data.count() == 1004  # 1004 log bayes factors
        assert set(data.columns) == {
            "molecular_trait_id",
            "region",
            "variant",
            *[f"lbf_variable{i}" for i in range(1, 11)],
            "dataset_id",
        }

    def test__read_credible_set_from_source(
        self: TestEqtlCatalogueFinemapping,
    ) -> None:
        """Test reading credible set from source."""
        data = EqtlCatalogueFinemapping.read_credible_set_from_source(
            "tests/gentropy/data_samples/QTD000001/QTS00001/QTS000001.credible_set.parquet",
            session=self.session,
        )
        assert isinstance(data, DataFrame)
        assert data.count() == 15  # 15 credible sets
        assert set(data.columns) == {
            "molecular_trait_id",
            "chromosome",
            "position",
            "variant",
            "pvalue",
            "beta",
            "se",
            "pip",
            "cs_id",
            "region",
            "gene_id",
            "dataset_id",
            "credibleSetIndex",
        }

    def test__read_credible_set_from_source_filters_non_atgc_alleles(
        self: TestEqtlCatalogueFinemapping, tmp_path: Path
    ) -> None:
        """Test that credible set variants with non-ATGC alt alleles (large indels/CNVs) are dropped.

        The raw eQTL Catalogue r8 credible sets for AFR_LCL (QTS000044) and MAGE (QTS000055)
        contain rows where the same locus is reported with imprecise coordinates and a CNV-notation
        `alt` (e.g. `<DEL>`, `<DUP>`), which produced duplicate credible-set entries. These rows
        must be filtered out, while normal ATGC variants -- including duplicates coming from the
        rsid explosion that `distinct()` is expected to collapse -- must be preserved.
        """
        mock_credible_sets = self.session.spark.createDataFrame(
            [
                # Same variant exploded across two rsids: must collapse to a single row.
                (
                    "ENSG00000001",
                    "1",
                    100,
                    "A",
                    "G",
                    "chr1_100_A_G",
                    10,
                    0.1,
                    0.001,
                    0.5,
                    0.1,
                    "SNP",
                    5,
                    "1",
                    "ENSG00000001",
                    "ENSG00000001",
                    0.0,
                    "rs1",
                    "ENSG00000001_L1",
                    3,
                    0.6,
                    3.0,
                    0.8,
                    "1:1-200",
                ),
                (
                    "ENSG00000001",
                    "1",
                    100,
                    "A",
                    "G",
                    "chr1_100_A_G",
                    10,
                    0.1,
                    0.001,
                    0.5,
                    0.1,
                    "SNP",
                    5,
                    "1",
                    "ENSG00000001",
                    "ENSG00000001",
                    0.0,
                    "rs2",
                    "ENSG00000001_L1",
                    3,
                    0.6,
                    3.0,
                    0.8,
                    "1:1-200",
                ),
                # Large deletion reported in CNV notation: must be dropped.
                (
                    "ENSG00000001",
                    "1",
                    150,
                    "ATGCATGC",
                    "<DEL>",
                    "chr1_150_ATGCATGC_<DEL>",
                    10,
                    0.1,
                    0.01,
                    0.2,
                    0.1,
                    "INDEL",
                    5,
                    "1",
                    "ENSG00000001",
                    "ENSG00000001",
                    0.0,
                    "rs3",
                    "ENSG00000001_L1",
                    3,
                    0.3,
                    1.0,
                    0.8,
                    "1:1-200",
                ),
                # Duplication reported in CNV notation: must be dropped.
                (
                    "ENSG00000001",
                    "1",
                    160,
                    "A",
                    "<DUP>",
                    "chr1_160_A_<DUP>",
                    10,
                    0.1,
                    0.02,
                    0.1,
                    0.1,
                    "CNV",
                    5,
                    "1",
                    "ENSG00000001",
                    "ENSG00000001",
                    0.0,
                    "rs4",
                    "ENSG00000001_L1",
                    3,
                    0.2,
                    0.5,
                    0.8,
                    "1:1-200",
                ),
            ],
            schema=EqtlCatalogueFinemapping.raw_credible_set_schema,
        )
        credible_set_path = tmp_path / "QTD000002" / "mock.credible_set.parquet"
        mock_credible_sets.write.parquet(str(credible_set_path))

        data = EqtlCatalogueFinemapping.read_credible_set_from_source(
            str(credible_set_path), session=self.session
        )

        # Only the deduplicated ATGC SNP survives: the exploded rsid duplicate is
        # collapsed by distinct() and the <DEL>/<DUP> rows are dropped by the ATGC filter.
        assert data.count() == 1
        row = data.collect()[0]
        assert row["variant"] == "chr1_100_A_G"
        # ref/alt are only needed to apply the filter and are dropped from the output.
        assert "ref" not in data.columns
        assert "alt" not in data.columns

    def test__parse_susie_results(
        self: TestEqtlCatalogueFinemapping,
        processed_finemapping_df: DataFrame,
    ) -> None:
        """Test parsing SuSIE results."""
        assert isinstance(processed_finemapping_df, DataFrame)
        assert set(processed_finemapping_df.columns) == {
            "variantId",
            "region",
            "chromosome",
            "position",
            "posteriorProbability",
            "pValueMantissa",
            "pValueExponent",
            "nSamples",
            "beta",
            "standardError",
            "credibleSetIndex",
            "logBF",
            "finemappingMethod",
            "traitFromSource",
            "geneId",
            "dataset_id",
            "studyId",
            "biosampleFromSourceId",
            "studyType",
            "projectId",
            "summarystatsLocation",
            "hasSumstats",
            "molecular_trait_id",
            "pubmedId",
            "condition",
        }
        assert processed_finemapping_df.count() == 15  # 15 credible sets

    def test__from_susie_results(self, processed_finemapping_df: DataFrame) -> None:
        """Test creating a study index from SuSIE results and uniqueness."""
        data = EqtlCatalogueFinemapping.from_susie_results(processed_finemapping_df)
        assert isinstance(data, StudyLocus)

        find_discrepancies = data.df.select(
            f.size("locus").alias("locus_size"),
            f.size(f.array_distinct("locus")).alias("locus_distinct_size"),
        ).filter(f.col("locus_size") != f.col("locus_distinct_size"))
        assert find_discrepancies.count() == 0


def test_study_identifier_sanitisation(processed_finemapping_df: DataFrame) -> None:
    """Test that the study identifiers are sanitised."""
    replaced_characters = r"[\+\:]"
    df = processed_finemapping_df.withColumn(
        "molecular_trait_id", f.lit("ENSG00000123456:ENST00000123456+ENST00000123457")
    )

    # Assert the presence of the characters that need to be replaced in the source data:
    assert df.filter(f.col("molecular_trait_id").rlike(replaced_characters)).count() > 0

    # Assert the absence of the characters that need to be replaced in the study index:
    assert (
        EqtlCatalogueStudyIndex.from_susie_results(df)
        .df.filter(f.col("studyId").rlike(replaced_characters))
        .count()
        == 0
    )
