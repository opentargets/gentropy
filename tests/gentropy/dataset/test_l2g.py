"""Tests on L2G datasets."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType

from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.l2g_prediction import L2GPrediction
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.study_locus_overlap import StudyLocusOverlap

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_feature_matrix(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test L2G Feature Matrix creation with mock data."""
    assert isinstance(mock_l2g_feature_matrix, L2GFeatureMatrix)


def test_gold_standard(mock_l2g_gold_standard: L2GFeatureMatrix) -> None:
    """Test L2G gold standard creation with mock data."""
    assert isinstance(mock_l2g_gold_standard, L2GGoldStandard)


def test_process_gene_interactions(sample_otp_interactions: DataFrame) -> None:
    """Tests processing of gene interactions from OTP."""
    expected_cols = ["geneIdA", "geneIdB", "score"]
    observed_df = L2GGoldStandard.process_gene_interactions(sample_otp_interactions)
    assert observed_df.columns == expected_cols, (
        "Gene interactions has a different schema."
    )


def test_predictions(mock_l2g_predictions: L2GPrediction) -> None:
    """Test L2G predictions creation with mock data."""
    assert isinstance(mock_l2g_predictions, L2GPrediction)


def test_filter_unique_associations(spark: SparkSession) -> None:
    """Test filter_unique_associations."""
    mock_l2g_gs_df = spark.createDataFrame(
        [
            ("1", "variant1", "study1", "gene1", "positive"),
            (
                "2",
                "variant2",
                "study1",
                "gene1",
                "negative",
            ),  # in the same locus as sl1 and pointing to same gene, has to be dropped
            (
                "3",
                "variant3",
                "study1",
                "gene1",
                "positive",
            ),  # in diff locus as sl1 and pointing to same gene, has to be kept
            (
                "4",
                "variant4",
                "study1",
                "gene2",
                "positive",
            ),  # in same locus as sl1 and pointing to diff gene, has to be kept
        ],
        "studyLocusId STRING, variantId STRING, studyId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_sl_overlap_df = spark.createDataFrame(
        [
            ("1", "2", "eqtl", "CHROM1", "variant2", None),
            ("1", "4", "eqtl", "CHROM1", "variant4", None),
        ],
        StudyLocusOverlap.get_schema(),
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "variant1", "study1", "gene1", "positive"),
            ("3", "variant3", "study1", "gene1", "positive"),
            ("4", "variant4", "study1", "gene2", "positive"),
        ],
        "studyLocusId STRING, variantId STRING, studyId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_l2g_gs = L2GGoldStandard(
        _df=mock_l2g_gs_df, _schema=L2GGoldStandard.get_schema()
    )
    mock_sl_overlap = StudyLocusOverlap(
        _df=mock_sl_overlap_df, _schema=StudyLocusOverlap.get_schema()
    )._convert_to_square_matrix()

    observed_df = mock_l2g_gs.filter_unique_associations(mock_sl_overlap).df

    assert observed_df.collect() == expected_df.collect()


def test_remove_false_negatives(spark: SparkSession) -> None:
    """Test `remove_false_negatives`."""
    mock_l2g_gs_df = spark.createDataFrame(
        [
            ("1", "variant1", "study1", "gene1", "positive"),
            (
                "2",
                "variant2",
                "study1",
                "gene2",
                "negative",
            ),  # gene2 is a partner of gene1, has to be dropped
            (
                "3",
                "variant3",
                "study1",
                "gene3",
                "negative",
            ),  # gene 3 is not a partner of gene1, has to be kept
            (
                "4",
                "variant4",
                "study1",
                "gene4",
                "positive",
            ),  # gene 4 is a partner of gene1, has to be kept because it's positive
        ],
        "studyLocusId STRING, variantId STRING, studyId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_interactions_df = spark.createDataFrame(
        [
            ("gene1", "gene2", 0.8),
            ("gene1", "gene3", 0.5),
            ("gene1", "gene4", 0.8),
        ],
        "geneIdA STRING, geneIdB STRING, score DOUBLE",
    )

    expected_df = spark.createDataFrame(
        [
            ("1", "variant1", "study1", "gene1", "positive"),
            ("3", "variant3", "study1", "gene3", "negative"),
            ("4", "variant4", "study1", "gene4", "positive"),
        ],
        "studyLocusId STRING, variantId STRING, studyId STRING, geneId STRING, goldStandardSet STRING",
    )

    mock_l2g_gs = L2GGoldStandard(
        _df=mock_l2g_gs_df, _schema=L2GGoldStandard.get_schema()
    )

    observed_df = mock_l2g_gs.remove_false_negatives(mock_interactions_df).df.orderBy(
        "studyLocusId"
    )

    assert observed_df.collect() == expected_df.collect()


def test_l2g_feature_constructor_with_schema_mismatch(
    spark: SparkSession,
) -> None:
    """Test if provided schema mismatch is converted to right type in the L2GFeatureMatrix constructor."""
    fm = L2GFeatureMatrix(
        _df=spark.createDataFrame(
            [
                ("1", "gene1", 100.0),
                ("2", "gene2", 1000.0),
            ],
            "studyLocusId STRING, geneId STRING, distanceTssMean DOUBLE",
        ),
        with_gold_standard=False,
    )
    assert fm._df.schema["distanceTssMean"].dataType == FloatType(), (
        "Feature `distanceTssMean` is not being casted to FloatType. Check L2GFeatureMatrix constructor."
    )


def test_calculate_feature_missingness_rate(
    mock_l2g_feature_matrix: L2GFeatureMatrix,
) -> None:
    """Test L2GFeatureMatrix.calculate_feature_missingness_rate."""
    expected_missingness = {
        "distanceTssMean": 0.0,
        "distanceSentinelTssMinimum": 0.0625,
    }
    observed_missingness = mock_l2g_feature_matrix.calculate_feature_missingness_rate()
    assert isinstance(observed_missingness, dict)
    assert mock_l2g_feature_matrix.features_list is not None and len(
        observed_missingness
    ) == len(mock_l2g_feature_matrix.features_list), (
        "Missing features in the missingness rate dictionary."
    )
    assert observed_missingness == expected_missingness, (
        "Missingness rate is incorrect."
    )


class TestEvidenceGeneration:
    """Test evidence generation from L2G dataset."""

    EVIDENCE_COLUMNS = [
        "datatypeId",
        "datasourceId",
        "targetFromSourceId",
        "diseaseFromSourceMappedId",
        "resourceScore",
        "studyLocusId",
        "literature",
    ]

    L2G_DATA = [("1", "gene1", 0.8), ("1", "gene2", 0.01)]

    STUDYLOCUS_DATA = [("1", "v1", "s1"), ("2", "v2", "s1")]

    STUDY_DATA = [
        ("s1", "p1", "gwas", "EFO_1,EFO_2", "22"),
        ("s2", "p1", "gwas", "EFO_1", "234"),
    ]

    @pytest.fixture(autouse=True)
    def _setup(self: TestEvidenceGeneration, spark: SparkSession) -> None:
        """Setup fixture."""
        self.study_index = StudyIndex(
            spark.createDataFrame(
                self.STUDY_DATA,
                "studyId STRING, projectId STRING, studyType STRING, diseaseIds STRING, pubmedId STRING",
            ).withColumn("diseaseIds", f.split("diseaseIds", ","))
        )

        # Study index is missing an optional column, which is required for evidence generation.
        self.study_index_no_disease = StudyIndex(
            spark.createDataFrame(
                self.STUDY_DATA,
                "studyId STRING, projectId STRING, studyType STRING, diseaseIds STRING, pubmedId STRING",
            ).drop("diseaseIds")
        )

        # Study index is missing an optional column, which is NOT required for evidence generation.
        self.study_index_no_pubmed = StudyIndex(
            spark.createDataFrame(
                self.STUDY_DATA,
                "studyId STRING, projectId STRING, studyType STRING, diseaseIds STRING, pubmedId STRING",
            )
            .withColumn("diseaseIds", f.split("diseaseIds", ","))
            .drop("pubmedId")
        )

        self.study_locus = StudyLocus(
            spark.createDataFrame(
                self.STUDYLOCUS_DATA,
                "studyLocusId STRING, variantId STRING, studyId STRING",
            )
        )

        self.l2g = L2GPrediction(
            spark.createDataFrame(
                self.L2G_DATA, "studyLocusId STRING, geneId STRING, score DOUBLE"
            )
        )

        # Evidence with one disease target evidence:
        self.evidence1 = self.l2g.to_disease_target_evidence(
            study_index=self.study_index, study_locus=self.study_locus
        )

        # Data with no evidence, because the l2g cutoff is too high:
        self.evidence2 = self.l2g.to_disease_target_evidence(
            study_index=self.study_index,
            study_locus=self.study_locus,
            l2g_threshold=2.0,
        )

        # Optional column is missing, but evidence is generated:
        self.evidence3 = self.l2g.to_disease_target_evidence(
            study_index=self.study_index_no_pubmed, study_locus=self.study_locus
        )

    def test_missing_optional_columns(self: TestEvidenceGeneration) -> None:
        """Test behaviour if optional columns are missing."""
        with pytest.raises(ValueError) as excinfo:
            self.l2g.to_disease_target_evidence(
                study_index=self.study_index_no_disease,
                study_locus=self.study_locus,
            )
        assert (
            "DisaseIds column has to be in the study index to generate disase/target evidence."
            in str(excinfo.value)
        )

    def test_evidence_type(self: TestEvidenceGeneration) -> None:
        """Test return type of the evidence generation process."""
        assert isinstance(self.evidence1, DataFrame)
        assert isinstance(self.evidence2, DataFrame)
        assert isinstance(self.evidence3, DataFrame)

    def test_evidence_length(self: TestEvidenceGeneration) -> None:
        """Test the length of the returned evidence table."""
        assert self.evidence1 is not None
        assert self.evidence2 is not None
        assert self.evidence1.count() >= 1
        assert self.evidence2.count() == 0

    def test_evidence_columns(self: TestEvidenceGeneration) -> None:
        """Test if the expected columns are in the returned dataset."""
        # Assert returned columns:
        if self.evidence1 is not None:
            for column in self.EVIDENCE_COLUMNS:
                assert column in self.evidence1.columns, (
                    f'Column "{column}" is missing from evidence dataset.'
                )
