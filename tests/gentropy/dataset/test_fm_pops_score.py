"""Tests for the fmPops score dataset."""

from __future__ import annotations

import math

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.dataset import Dataset
from gentropy.dataset.fm_pops_score import FmPopsScore
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.target_index import TargetIndex
from gentropy.dataset.variant_index import VariantIndex


def test_fm_pops_score_creation(mock_fm_pops_score: FmPopsScore) -> None:
    """Test that the mock fmPops score is a Dataset."""
    assert isinstance(mock_fm_pops_score, Dataset)


class TestFmPopsScoreInputs:
    """Test the target and covariates fmPops is fitted on.

    Three protein-coding genes and one lncRNA on chromosome 1. Variant v1 is nearest to geneA
    once the lncRNA is set aside, and variant v2 is as close to geneA as to geneC.
    """

    @pytest.fixture()
    def target_index(self: TestFmPopsScoreInputs, spark: SparkSession) -> TargetIndex:
        """Genes with positions, all within 500 kb of each other."""
        return TargetIndex(
            _df=spark.createDataFrame(
                [
                    ("geneA", "protein_coding", "1", 1_000_000, 1_009_999, 1_000_000),
                    ("geneB", "protein_coding", "1", 1_100_000, 1_100_999, 1_100_000),
                    ("geneC", "protein_coding", "1", 1_004_000, 1_004_999, 1_004_000),
                    ("geneL", "lncRNA", "1", 1_001_000, 1_001_999, 1_001_000),
                ],
                "id string, biotype string, chromosome string, start long, end long, tss long",
            ).select(
                "id",
                "biotype",
                f.struct("chromosome", "start", "end").alias("genomicLocation"),
                "tss",
            ),
            _schema=TargetIndex.get_schema(),
        )

    @pytest.fixture()
    def variant_index(self: TestFmPopsScoreInputs, spark: SparkSession) -> VariantIndex:
        """Two lead variants with their distances to the genes."""
        consequence = StructType(
            [
                StructField("distanceFromFootprint", LongType(), True),
                StructField("distanceFromTss", LongType(), True),
                StructField("targetId", StringType(), True),
                StructField("isEnsemblCanonical", BooleanType(), True),
                StructField("biotype", StringType(), True),
            ]
        )
        schema = StructType(
            [
                StructField("variantId", StringType(), True),
                StructField("chromosome", StringType(), True),
                StructField("position", IntegerType(), True),
                StructField("referenceAllele", StringType(), True),
                StructField("alternateAllele", StringType(), True),
                StructField("transcriptConsequences", ArrayType(consequence), True),
            ]
        )
        return VariantIndex(
            _df=spark.createDataFrame(
                [
                    (
                        "v1",
                        "1",
                        1_001_000,
                        "A",
                        "T",
                        [
                            (0, 1_000, "geneA", True, "protein_coding"),
                            (0, 99_000, "geneB", True, "protein_coding"),
                            (0, 10, "geneL", True, "lncRNA"),
                        ],
                    ),
                    (
                        "v2",
                        "1",
                        1_002_000,
                        "A",
                        "T",
                        [
                            (0, 2_000, "geneA", True, "protein_coding"),
                            (0, 2_000, "geneC", True, "protein_coding"),
                            (0, 98_000, "geneB", True, "protein_coding"),
                        ],
                    ),
                ],
                schema,
            ),
            _schema=VariantIndex.get_schema(),
        )

    @pytest.fixture()
    def study_index(self: TestFmPopsScoreInputs, spark: SparkSession) -> StudyIndex:
        """Three GWAS studies and a molecular QTL study."""
        return StudyIndex(
            _df=spark.createDataFrame(
                [
                    ("s1", "gwas", "p", ["d1", "d2"]),
                    ("s2", "gwas", "p", ["d1"]),
                    ("s3", "gwas", "p", ["d3"]),
                    ("s4", "eqtl", "p", ["d4"]),
                ],
                "studyId string, studyType string, projectId string, diseaseIds array<string>",
            ),
            _schema=StudyIndex.get_schema(),
        )

    @pytest.fixture()
    def study_locus(self: TestFmPopsScoreInputs, spark: SparkSession) -> StudyLocus:
        """One credible set per study."""
        return StudyLocus(
            _df=spark.createDataFrame(
                [
                    ("cs1", "s1", "v1", "gwas"),
                    ("cs2", "s2", "v1", "gwas"),
                    ("cs3", "s3", "v2", "gwas"),
                    ("cs4", "s4", "v2", "eqtl"),
                ],
                "studyLocusId string, studyId string, variantId string, studyType string",
            ).withColumn("chromosome", f.lit("1")),
            _schema=StudyLocus.get_schema(),
        )

    def test_disease_counts_per_nearest_gene(
        self: TestFmPopsScoreInputs,
        study_locus: StudyLocus,
        study_index: StudyIndex,
        variant_index: VariantIndex,
        target_index: TargetIndex,
    ) -> None:
        """Diseases count once per gene, ties keep both genes and QTL studies do not count."""
        observed = {
            row["geneId"]: row["targetCount"]
            for row in FmPopsScore.disease_counts_per_nearest_gene(
                study_locus, study_index, variant_index, target_index
            ).collect()
        }
        # geneA: d1 and d2 through cs1, d1 again through cs2, d3 through the tie at cs3.
        # geneC: d3 through the tie. geneB is never nearest; d4 comes from an eQTL study.
        assert observed == {"geneA": 3, "geneC": 1}

    def test_gene_covariates(
        self: TestFmPopsScoreInputs, target_index: TargetIndex
    ) -> None:
        """Gene length and the number of neighbouring protein-coding genes, logged."""
        observed = {
            row["geneId"]: (row["logGeneLength"], row["logNeighbouringGenes"])
            for row in FmPopsScore.gene_covariates(target_index).collect()
        }
        assert set(observed) == {"geneA", "geneB", "geneC"}
        assert observed["geneA"] == pytest.approx((math.log(10_000), math.log(3)))
        assert observed["geneB"] == pytest.approx((math.log(1_000), math.log(3)))
        nearby = FmPopsScore.gene_covariates(target_index, genomic_window=50_000)
        assert {
            row["geneId"]: row["logNeighbouringGenes"] for row in nearby.collect()
        } == pytest.approx({"geneA": math.log(2), "geneB": 0.0, "geneC": math.log(2)})
