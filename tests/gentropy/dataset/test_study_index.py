"""Test study index dataset."""

from __future__ import annotations

import pytest
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.study_index import StudyIndex
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


def test_study_index_creation(mock_study_index: StudyIndex) -> None:
    """Test study index creation with mock data."""
    assert isinstance(mock_study_index, StudyIndex)


def test_study_index_type_lut(mock_study_index: StudyIndex) -> None:
    """Test study index type lut."""
    assert isinstance(mock_study_index.study_type_lut(), DataFrame)


def test_aggregate_and_map_ancestries__correctness(spark: SparkSession) -> None:
    """Test if population are mapped and relative sample sizes are calculated."""
    data = [
        (
            "s1",
            "East Asian",
            100,
        ),
        (
            "s1",
            "Finnish",
            100,
        ),
        (
            "s1",
            "NR",
            100,
        ),
        (
            "s1",
            "European",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")).alias(
                "parsedPopulation"
            )
        )
    )

    # Asserting that there are three population (both NR and Europeans are grounded to 'nfe'):
    assert (df.select(f.explode("parsedPopulation")).count()) == 3

    # Asserting that the relative count go to 1.0
    assert (
        (
            df.select(
                f.aggregate(
                    "parsedPopulation",
                    f.lit(0.0),
                    lambda y, x: y + x.relativeSampleSize,
                ).alias("sum")
            ).collect()[0]["sum"]
        )
        == 1.0
    )


def test_aggregate_samples_by_ancestry__correctness(spark: SparkSession) -> None:
    """Test correctness of the ancestry aggregator function."""
    data = [
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a1",
            100,
        ),
        (
            "s1",
            "a2",
            100,
        ),
    ]

    columns = ["studyId", "ancestry", "sampleSize"]

    df = (
        spark.createDataFrame(data, columns)
        .groupBy("studyId")
        .agg(
            f.collect_list(f.struct("ancestry", "sampleSize")).alias("discoverySamples")
        )
        .select(
            f.aggregate(
                "discoverySamples",
                f.array_distinct(
                    f.transform(
                        "discoverySamples",
                        lambda x: f.struct(
                            x.ancestry.alias("ancestry"), f.lit(0.0).alias("sampleSize")
                        ),
                    )
                ),
                StudyIndex._aggregate_samples_by_ancestry,
            ).alias("test_output")
        )
        .persist()
    )

    # Asserting the number of aggregated population:
    assert (
        df.filter(f.col("studyId") == "s1").select(f.explode("test_output")).count()
    ) == 2

    # Asserting the number of aggregated sample size:
    assert (
        (
            df.filter(f.col("studyId") == "s1")
            .select(
                f.aggregate(
                    "test_output", f.lit(0.0), lambda y, x: x.sampleSize + y
                ).alias("totalSamples")
            )
            .collect()[0]["totalSamples"]
        )
        == 300.0
    )


class TestGeneValidation:
    """A small test suite to ensure the gene validation works as intended."""

    GENE_DATA = [
        ("ENSG00000102021", "1"),
        ("ENSG000001020", "1"),
    ]

    GENE_COLUMNS = ["geneId", "chromosome"]

    STUDY_DATA = [
        ("s1", "eqtl", "p", "ENSG00000102021"),
        # This is the only study to be flagged: QTL + Wrong gene
        ("s2", "eqtl", "p", "cicaful"),
        ("s3", "gwas", "p", None),
        ("s4", "gwas", "p", "pocok"),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "geneId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestGeneValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array()
            ),
            _schema=StudyIndex.get_schema(),
        )

        self.study_index_no_gene = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS)
            .withColumn("qualityControls", f.array())
            .drop("geneId"),
            _schema=StudyIndex.get_schema(),
        )

        self.gene_index = GeneIndex(
            _df=spark.createDataFrame(self.GENE_DATA, self.GENE_COLUMNS),
            _schema=GeneIndex.get_schema(),
        )

    def test_gene_validation_type(self: TestGeneValidation) -> None:
        """Testing if the validation runs and returns the expected type."""
        validated = self.study_index.validate_target(self.gene_index)
        assert isinstance(validated, StudyIndex)

    def test_gene_validation_correctness(self: TestGeneValidation) -> None:
        """Testing if the gene validation only flags the expected studies."""
        validated = self.study_index.validate_target(self.gene_index).persist()

        # Make sure there's only one flagged:
        assert validated.df.filter(f.size("qualityControls") != 0).count() == 1

        # Make sure there's only one flagged:
        flagged_study = validated.df.filter(f.size("qualityControls") != 0).collect()[
            0
        ]["studyId"]

        assert flagged_study == "s2"

    def test_gene_validation_no_gene_column(self: TestGeneValidation) -> None:
        """Testing what happens if no geneId column is present."""
        validated = self.study_index_no_gene.validate_target(self.gene_index)

        # Asserty type:
        assert isinstance(validated, StudyIndex)

        # Assert count:
        assert validated.df.count() == self.study_index.df.count()


class TestUniquenessValidation:
    """A small test suite to ensure the gene validation works as intended."""

    STUDY_DATA = [
        # This is the only study to be flagged:
        ("s1", "eqtl", "p"),
        ("s1", "eqtl", "p"),
        ("s3", "gwas", "p"),
        ("s4", "gwas", "p"),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestUniquenessValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array()
            ),
            _schema=StudyIndex.get_schema(),
        )

    def test_uniqueness_return_type(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        assert isinstance(self.study_index.validate_unique_study_id(), StudyIndex)

    def test_uniqueness_correct_data(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        validated = self.study_index.validate_unique_study_id().persist()

        # We have more than one flagged studies:
        assert validated.df.filter(f.size(f.col("qualityControls")) > 0).count() > 1

        # The flagged study identifiers are found more than once:
        flagged_ids = {
            study["studyId"]: study["count"]
            for study in validated.df.filter(f.size(f.col("qualityControls")) > 0)
            .groupBy("studyId")
            .count()
            .collect()
        }

        for _, count in flagged_ids.items():
            assert count > 1

        # the right study is found:
        assert "s1" in flagged_ids
