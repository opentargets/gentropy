"""Test study index dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from gentropy.dataset.biosample_index import BiosampleIndex
from gentropy.dataset.study_index import (
    StudyAnalysisFlag,
    StudyIndex,
    StudyQualityCheck,
)
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC
from gentropy.dataset.target_index import TargetIndex

if TYPE_CHECKING:
    from typing import Any


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


class TestQTLValidation:
    """A small test suite to ensure the QTL study validation works as intended."""

    GENE_DATA = [
        ("ENSG00000102021", "protein_coding"),
        ("ENSG000001020", "lncRNA"),
    ]
    GENE_COLUMNS = ["id", "biotype"]

    BIOSAMPLE_DATA = [("UBERON_00123", "lung"), ("CL_00321", "monocyte")]
    BIOSAMPLE_COLUMNS = ["biosampleId", "biosampleName"]

    STUDY_DATA = [
        ("s1", "eqtl", "p", "ENSG00000102021", "UBERON_00123"),
        # This is the only study to be flagged: QTL + Wrong gene
        ("s2", "eqtl", "p", "cicaful", "UBERON_00123"),
        # This is the only study to be flagged: QTL + Wrong biosample
        ("s3", "sqtl", "p", "ENSG00000102021", "jibberish"),
        ("s4", "gwas", "p", None, "anything"),
        ("s5", "gwas", "p", "pocok", None),
    ]
    STUDY_COLUMNS = [
        "studyId",
        "studyType",
        "projectId",
        "geneId",
        "biosampleFromSourceId",
    ]

    @pytest.fixture(autouse=True)
    def _setup(self: TestQTLValidation, spark: SparkSession) -> None:
        """Setup fixture."""

        def create_study_index(drop_column: str | None = None) -> StudyIndex:
            df = spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS)
            df = df.withColumn("qualityControls", f.array().cast("array<string>"))
            if drop_column:
                df = df.drop(drop_column)
            return StudyIndex(_df=df, _schema=StudyIndex.get_schema())

        self.study_index = create_study_index()
        self.study_index_no_gene = create_study_index("geneId")
        self.study_index_no_biosample_id = create_study_index("biosampleFromSourceId")

        self.target_index = TargetIndex(
            _df=spark.createDataFrame(self.GENE_DATA, self.GENE_COLUMNS),
            _schema=TargetIndex.get_schema(),
        )
        self.biosample_index = BiosampleIndex(
            _df=spark.createDataFrame(self.BIOSAMPLE_DATA, self.BIOSAMPLE_COLUMNS),
            _schema=BiosampleIndex.get_schema(),
        )

    def test_gene_validation_type(self: TestQTLValidation) -> None:
        """Testing if the target validation runs and returns the expected type."""
        validated = self.study_index.validate_target(self.target_index)
        assert isinstance(validated, StudyIndex)

    def test_biosample_validation_type(self: TestQTLValidation) -> None:
        """Testing if the biosample validation runs and returns the expected type."""
        validated = self.study_index.validate_biosample(self.biosample_index)
        assert isinstance(validated, StudyIndex)

    @pytest.mark.parametrize(
        ["test", "bad_study"],
        [
            pytest.param("gene", "s2", id="gene"),
            pytest.param("biosample", "s3", id="biosample"),
        ],
    )
    def test_qtl_validation_correctness(
        self: TestQTLValidation, test: str, bad_study: str
    ) -> None:
        """Testing if the QTL validation only flags the expected studies."""
        match test:
            case "gene":
                validated = self.study_index.validate_target(
                    self.target_index
                ).persist()
            case "biosample":
                validated = self.study_index.validate_biosample(
                    self.biosample_index
                ).persist()
            case _:
                raise ValueError(f"Unknown test type: {test}")
        # Make sure there's only one flagged:
        assert validated.df.filter(f.size("qualityControls") != 0).count() == 1

        # Make sure there's only one flagged:
        flagged_study = validated.df.filter(f.size("qualityControls") != 0).collect()[
            0
        ]["studyId"]
        assert flagged_study == bad_study

    @pytest.mark.parametrize(
        ["drop", "test"],
        [
            pytest.param(
                "gene",
                "gene",
                id="validate target, no geneId column",
            ),
            pytest.param(
                "gene",
                "biosample",
                id="validate biosample, no geneId column",
            ),
            pytest.param(
                "biosample",
                "biosample",
                id="validate biosample, no biosampleFromSourceId column",
            ),
            pytest.param(
                "biosample",
                "gene",
                id="validate target, no biosampleFromSourceId column",
            ),
        ],
    )
    def test_qtl_validation_drop_relevant_column(
        self: TestQTLValidation, drop: str, test: str
    ) -> None:
        """Testing what happens if an expected column is not present."""
        match drop:
            case "gene":
                study_index = self.study_index_no_gene
            case "biosample":
                study_index = self.study_index_no_biosample_id
            case _:
                raise ValueError(f"Unknown drop type: {drop}")
        match test:
            case "gene":
                validated = study_index.validate_target(self.target_index)
            case "biosample":
                validated = study_index.validate_biosample(self.biosample_index)
            case _:
                raise ValueError(f"Unknown test type: {test}")

        # Assert type:
        assert isinstance(validated, StudyIndex)

        # Assert count:
        assert validated.df.count() == self.study_index.df.count()

    def test_qtl_validation_existing_biosample_column(self: TestQTLValidation) -> None:
        """Testing what happens if a biosampleId column is present in study index as well as biosampleFromSourceId."""
        # Append a biosample column filled with null to the self.study_index then validate:
        validated = StudyIndex(
            _df=self.study_index.df.withColumn(
                "biosampleId", f.lit(None).cast("string")
            ),
            _schema=StudyIndex.get_schema(),
        ).validate_biosample(self.biosample_index)
        assert isinstance(validated, StudyIndex)


class TestUniquenessValidation:
    """A small test suite to ensure the gene validation works as intended."""

    STUDY_DATA = [
        # This is the only study to be flagged:
        ("s1", "eqtl", "p"),
        ("s1", "eqtl", "p"),  # Duplicate -> one should be flagged
        ("s3", "gwas", "p"),
        ("s4", "gwas", "p"),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestUniquenessValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array().cast("array<string>")
            ),
            _schema=StudyIndex.get_schema(),
        )

    def test_uniqueness_return_type(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        assert isinstance(self.study_index.validate_unique_study_id(), StudyIndex)

    def test_uniqueness_correct_data(self: TestUniquenessValidation) -> None:
        """Testing if the function returns the right type."""
        validated = self.study_index.validate_unique_study_id().persist()

        # We have only one flagged study:
        assert validated.df.filter(f.size(f.col("qualityControls")) > 0).count() == 1

        # The flagged study identifiers are found more than once:
        flagged_ids = {
            study["studyId"]: study["count"]
            for study in validated.df.filter(f.size(f.col("qualityControls")) > 0)
            .groupBy("studyId")
            .count()
            .collect()
        }

        for _, count in flagged_ids.items():
            assert count == 1

        # the right study is found:
        assert "s1" in flagged_ids


class TestStudyTypeValidation:
    """Testing study type validation."""

    STUDY_DATA = [
        # This study is flagged because of unexpected type:
        ("s1", "cicaful", "p", "gene1"),
        ("s3", "eqtl", "p", "gene1"),
        ("s4", "gwas", "p", None),
    ]
    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "geneId"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyTypeValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            _df=spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS).withColumn(
                "qualityControls", f.array().cast("array<string>")
            ),
            _schema=StudyIndex.get_schema(),
        )

    def test_study_type_validation_return_type(self: TestStudyTypeValidation) -> None:
        """Testing if the function returns the expected type."""
        assert isinstance(self.study_index.validate_study_type(), StudyIndex)

    def test_study_type_validation_correctness(self: TestStudyTypeValidation) -> None:
        """Test if the correct study is flagged."""
        flagged_study_ids = [
            study["studyId"]
            for study in self.study_index.validate_study_type()
            .df.filter(f.size("qualityControls") > 0)
            .collect()
        ]
        assert "s1" in flagged_study_ids
        # Check if any
        flagged_study_types = [
            study["studyType"]
            for study in self.study_index.validate_study_type()
            .df.filter(f.size("qualityControls") != 0)
            .collect()
        ]
        for study_type in flagged_study_types:
            assert study_type != "gwas"
            assert "qtl" not in study_type


class TestDiseaseValidation:
    """Testing the disease validation."""

    DISEASE_DATA = [
        ("EFO_old", "EFO_new"),
        ("EFO_new", "EFO_new"),
        ("EFO_new2", "EFO_new2"),
    ]

    DISEASE_HEADER = ["efo", "diseaseId"]

    STUDY_DATA = [
        # Old EFO mapped to new:
        ("s1", "gwas", "p", "EFO_old"),
        # List of EFOs some mapped, some not:
        ("s2", "gwas", "p", "EFO_old"),
        ("s2", "gwas", "p", "EFO_new2"),
        ("s2", "gwas", "p", "EFO_invalid"),
        # single EFO mapped as the same:
        ("s3", "gwas", "p", "EFO_new2"),
        # Invalid study:
        ("s4", "gwas", "p", "EFO_invalid"),
        # Invalid study - no EFO:
        ("s5", "gwas", "p", None),
        # Valid study, missing efo, not gwas:
        ("s6", "eqtl", "p2", None),
    ]

    STUDY_COLUMNS = ["studyId", "studyType", "projectId", "efo"]

    @pytest.fixture(autouse=True)
    def _setup(self: TestDiseaseValidation, spark: SparkSession) -> None:
        """Setup fixture."""
        study_df = (
            spark.createDataFrame(self.STUDY_DATA, self.STUDY_COLUMNS)
            .groupBy("studyId", "studyType", "projectId")
            .agg(f.collect_set("efo").alias("traitFromSourceMappedIds"))
            .withColumn("qualityControls", f.array().cast("array<string>"))
            .withColumn(
                "backgroundTraitFromSourceMappedIds", f.array().cast("array<string>")
            )
        )
        # Mock study index:
        self.study_index = StudyIndex(
            _df=study_df,
            _schema=StudyIndex.get_schema(),
        )

        # Disease mapping:
        self.disease = spark.createDataFrame(self.DISEASE_DATA, self.DISEASE_HEADER)

        # Validated data:
        self.validated = self.study_index.validate_disease(self.disease).persist()

    def test_disease_validation_return_type(self: TestDiseaseValidation) -> None:
        """Testing if the disease validation returns the right type."""
        assert isinstance(self.validated, StudyIndex)

    def test_disease_validation_right_flag(self: TestDiseaseValidation) -> None:
        """Testing if the right studies are flagged in the validation step."""
        # Testing flagged studies:
        for study in self.validated.df.filter(f.size("qualityControls") > 0).collect():
            # All flagged studies are from gwas:
            assert study["studyType"] == "gwas"
            # None of the flagged studies have assigned valid disease:
            assert len(study["diseaseIds"]) == 0

        # Testing unflagged studies:
        for study in self.validated.df.filter(f.size("qualityControls") == 0).collect():
            # If a valid study has no disease, it cannot be gwas:
            if len(study["diseaseIds"]) == 0:
                assert study["studyType"] != "gwas"

    def test_disease_validation_disease_mapping(self: TestDiseaseValidation) -> None:
        """Testing if old disease identifiers can be rescued."""
        example_study_id = "s1"

        test_study = self.validated.df.filter(
            f.col("studyId") == example_study_id
        ).collect()[0]

        # Assert validation:
        assert len(test_study["qualityControls"]) == 0

        # Assert disease mapping:
        assert test_study["traitFromSourceMappedIds"][0] != test_study["diseaseIds"][0]

    def test_disease_validation_disease_removal(self: TestDiseaseValidation) -> None:
        """Testing if not all diseases can be mapped, the study still passes QC."""
        example_study_id = "s2"

        test_study = self.validated.df.filter(
            f.col("studyId") == example_study_id
        ).collect()[0]

        # Assert validation:
        assert len(test_study["qualityControls"]) == 0

        # Assert not all diseases could be mapped to disease index:
        assert len(test_study["traitFromSourceMappedIds"]) > len(
            test_study["diseaseIds"]
        )


class TestStudyIndexAnnotation:
    """Test study index annotation."""

    STUDY_REQUIRED_DATA: list[tuple[str, str, str, list[str], bool]] = [
        ("s1", "GCST", "gwas", [], True),
        ("s2", "GCST", "gwas", [], True),
    ]

    # Data to represent the studyIndex with tophits (s3) and sumstats (24)
    STUDY_REQUIRED_DATA_NON_SUMSTATS: list[tuple[str, str, str, list[str], bool]] = [
        ("s3", "GCST", "gwas", [], False),
        ("s4", "GCST", "gwas", [], True),
    ]
    STUDY_REQUIRED_SCHEMA = "studyId STRING, projectId STRING, studyType STRING, qualityControls ARRAY<STRING>, hasSumstats boolean"

    STUDY_WITH_ANALYSIS_FLAGS: list[tuple[str, str, str, list[str], list[str]]] = [
        ("s1", "GCST", "gwas", [], [StudyAnalysisFlag.CASE_CASE_STUDY.value]),
        ("s2", "GCST", "gwas", [], [StudyAnalysisFlag.EXWAS.value]),
        (
            "s3",
            "GCST",
            "gwas",
            [],
            [StudyAnalysisFlag.CASE_CASE_STUDY.value, StudyAnalysisFlag.EXWAS.value],
        ),
        ("s4", "GCST", "gwas", [], []),
    ]
    STUDY_WITH_ANALYSIS_FLAGS_SCHEMA = "studyId STRING, projectId STRING, studyType STRING, qualityControls ARRAY<STRING>, analysisFlags ARRAY<STRING>"

    @pytest.fixture(autouse=True)
    def _setup(self: TestStudyIndexAnnotation, spark: SparkSession) -> None:
        """Setup fixture."""
        self.spark = spark
        self.thresholds: dict[str, Any] = {
            "threshold_mean_beta": 0.05,
            "threshold_mean_diff_pz": 0.05,
            "threshold_se_diff_pz": 0.05,
            "threshold_min_gc_lambda": 0.7,
            "threshold_max_gc_lambda": 2.5,
            "threshold_min_n_variants": 2,
        }

    def test_annotation_with_is_harmonised(
        self: TestStudyIndexAnnotation,
    ) -> None:
        """Test if qc annotation adds a flag to the study index if the study is missing from qc."""
        # Build a qc dataset
        qc_data = [
            ("s4", 0.01, 0.01, 0.01, 1.0, 2, 2),
        ]
        qc_schema = SummaryStatisticsQC.get_schema()
        qc_df = self.spark.createDataFrame(qc_data, qc_schema)
        qc = SummaryStatisticsQC(_df=qc_df)

        # Create a study index with one row from tophits and one from sumstats
        si_df = self.spark.createDataFrame(
            self.STUDY_REQUIRED_DATA_NON_SUMSTATS, self.STUDY_REQUIRED_SCHEMA
        )
        self.study_index = StudyIndex(_df=si_df)

        annotated_study_index = self.study_index.annotate_sumstats_qc(
            qc, **self.thresholds
        )

        # Assert that the study index is annotated correctly:
        assert isinstance(annotated_study_index, StudyIndex), "should be a StudyIndex"
        assert annotated_study_index.df.count() == 2, "Should have 2 rows"
        annotated = (
            annotated_study_index.df.filter(f.col("studyId") == f.lit("s3"))
            .select("qualityControls")
            .collect()
        )
        exp_flag = StudyQualityCheck.SUMSTATS_NOT_AVAILABLE.value
        assert annotated[0][0] == [exp_flag], "Should be annotated with the flag"
        non_annotated = (
            annotated_study_index.df.filter(f.col("studyId") == f.lit("s4"))
            .select("qualityControls")
            .collect()
        )
        assert non_annotated[0][0] == [], "Should not be annotated with the flag"

    @pytest.mark.parametrize(
        ["qc_data", "annotated_study", "qc_flag"],
        [
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", -0.06, 0.01, 0.01, 1.0, 2, 2),
                ],
                "s2",
                StudyQualityCheck.FAILED_MEAN_BETA_CHECK,
                id="s2 annotated due to abs(mean_beta) >= threshold",
            ),
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", 0.01, -0.06, 0.01, 1.0, 2, 2),
                ],
                "s2",
                StudyQualityCheck.FAILED_PZ_CHECK,
                id="s2 annotated due to abs(mean_diff_pz) >= threshold",
            ),
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", 0.01, 0.01, 0.06, 1.0, 2, 2),
                ],
                "s2",
                StudyQualityCheck.FAILED_PZ_CHECK,
                id="s2 annotated due to se_diff_pz >= threshold",
            ),
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", 0.01, 0.01, 0.01, 0.5, 2, 2),
                ],
                "s2",
                StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
                id="s2 annotated due to gc_lambda <= min_threshold",
            ),
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", 0.01, 0.01, 0.01, 3.0, 2, 2),
                ],
                "s2",
                StudyQualityCheck.FAILED_GC_LAMBDA_CHECK,
                id="s2 annotated due to gc_lambda >= max_threshold",
            ),
            pytest.param(
                [
                    ("s1", 0.01, 0.01, 0.01, 1.0, 2, 2),
                    ("s2", 0.01, 0.01, 0.01, 1.0, 1, 1),
                ],
                "s2",
                StudyQualityCheck.SMALL_NUMBER_OF_SNPS,
                id="s2 annotated due to n_variants < threshold",
            ),
        ],
    )
    def test_annotation_with_sumstat_qc(
        self: TestStudyIndexAnnotation,
        qc_data: list[tuple[str, float, float, float, float, int, int]],
        annotated_study: str,
        qc_flag: StudyQualityCheck,
    ) -> None:
        """Test study index annotation with quality control."""
        # Annotate study index with QC dataset:
        qc_schema = SummaryStatisticsQC.get_schema()
        qc_df = self.spark.createDataFrame(qc_data, qc_schema)
        qc = SummaryStatisticsQC(_df=qc_df)

        # Recreate the study index each time to keep the test isolated
        si_df = self.spark.createDataFrame(
            self.STUDY_REQUIRED_DATA, self.STUDY_REQUIRED_SCHEMA
        )
        self.study_index = StudyIndex(_df=si_df)

        annotated_study_index = self.study_index.annotate_sumstats_qc(
            qc, **self.thresholds
        )

        # Assert that the study index is annotated correctly:
        assert isinstance(annotated_study_index, StudyIndex), "should be a StudyIndex"
        assert "sumstatQCValues" in annotated_study_index.df.columns
        assert annotated_study_index.df.count() == 2, "Should have 2 rows"
        annotated = (
            annotated_study_index.df.filter(f.col("studyId") == f.lit(annotated_study))
            .select("qualityControls")
            .collect()
        )
        assert annotated[0][0] == [qc_flag.value], "Should be annotated with the flag"

        non_annotated = (
            annotated_study_index.df.filter(f.col("studyId") != f.lit(annotated_study))
            .select("qualityControls")
            .collect()
        )
        assert non_annotated[0][0] == [], "Should not be annotated with the flag"

    def test_validation_of_analysis_flags(
        self: TestStudyIndexAnnotation,
    ) -> None:
        """Test study index validation of studies based on the analysis flags.

        Test expects sample s1 and s3 to be annotated with the analysis flag 'case-case study'
        """
        # Craete a study index with analysis flags that contain the expected flag:
        si_df = self.spark.createDataFrame(
            self.STUDY_WITH_ANALYSIS_FLAGS, self.STUDY_WITH_ANALYSIS_FLAGS_SCHEMA
        )
        self.study_index = StudyIndex(_df=si_df)

        # Annotate study index with analysis flags:
        annotated_study_index = self.study_index.validate_analysis_flags()

        # Assert that the study index is annotated correctly:
        assert isinstance(annotated_study_index, StudyIndex), "should be a StudyIndex"
        assert annotated_study_index.df.count() == 4, "Should have 4 rows"
        exp_flag = StudyQualityCheck.CASE_CASE_STUDY_DESIGN.value
        annotated = (
            annotated_study_index.df.filter(
                f.array_contains("qualityControls", exp_flag)
            )
            .select("studyId")
            .orderBy("studyId")
            .collect()
        )
        assert annotated[0][0] == "s1", "s1 Should be annotated with the flag"
        assert annotated[1][0] == "s3", "s3 Should be annotated with the flag"
