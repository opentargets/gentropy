"""Test locus-to-gene model training."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from oxygen.dataset.colocalisation import Colocalisation
from oxygen.dataset.l2g_feature import L2GFeature
from oxygen.dataset.l2g_feature_matrix import L2GFeatureMatrix
from oxygen.dataset.study_index import StudyIndex
from oxygen.dataset.study_locus import StudyLocus
from oxygen.method.l2g.feature_factory import ColocalisationFactory, StudyLocusFactory
from oxygen.method.l2g.model import LocusToGeneModel
from oxygen.method.l2g.trainer import LocusToGeneTrainer
from pyspark.ml import PipelineModel
from pyspark.ml.tuning import ParamGridBuilder
from xgboost.spark import SparkXGBClassifier

if TYPE_CHECKING:
    from oxygen.dataset.v2g import V2G
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def model() -> LocusToGeneModel:
    """Creates an instance of the LocusToGene class."""
    estimator = SparkXGBClassifier(
        eval_metric="logloss",
        features_col="features",
        label_col="label",
        max_depth=5,
    )
    return LocusToGeneModel(estimator=estimator, features_list=["distanceTssMean"])


class TestLocusToGeneTrainer:
    """Test the L2GTrainer methods using a logistic regression model as estimation algorithm."""

    def test_cross_validate(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the k-fold cross-validation function."""
        param_grid = (
            ParamGridBuilder()
            .addGrid(model.estimator.learning_rate, [0.1, 0.01])
            .build()
        )
        best_model = LocusToGeneTrainer.cross_validate(
            model, mock_l2g_feature_matrix.fill_na(), num_folds=2, param_grid=param_grid
        )
        assert isinstance(
            best_model, LocusToGeneModel
        ), "Unexpected model type returned from cross_validate"
        # Check that the best model's hyperparameters are among those in the param_grid
        assert best_model.model.getOrDefault("learning_rate") in [  # type: ignore
            0.1,
            0.01,
        ], "Unexpected learning rate in the best model"

    def test_train(
        self: TestLocusToGeneTrainer,
        mock_l2g_feature_matrix: L2GFeatureMatrix,
        model: LocusToGeneModel,
    ) -> None:
        """Test the training function."""
        trained_model = LocusToGeneTrainer.train(
            mock_l2g_feature_matrix.fill_na(),
            model,
            features_list=["distanceTssMean"],
            evaluate=False,
        )
        # Check that `model` is a PipelineModel object and not None
        assert isinstance(
            trained_model.model, PipelineModel
        ), "Model is not a PipelineModel object."


class TestColocalisationFactory:
    """Test the ColocalisationFactory methods."""

    @pytest.mark.parametrize(
        "colocalisation_method",
        [
            "COLOC",
            "eCAVIAR",
        ],
    )
    def test_get_max_coloc_per_study_locus(
        self: TestColocalisationFactory,
        mock_study_locus: StudyLocus,
        mock_study_index: StudyIndex,
        mock_colocalisation: Colocalisation,
        colocalisation_method: str,
    ) -> None:
        """Test the function that extracts the maximum log likelihood ratio for each pair of overlapping study-locus returns the right data type."""
        coloc_features = ColocalisationFactory._get_max_coloc_per_study_locus(
            mock_study_locus,
            mock_study_index,
            mock_colocalisation,
            colocalisation_method,
        )
        assert isinstance(
            coloc_features, L2GFeature
        ), "Unexpected model type returned from _get_max_coloc_per_study_locus"

    def test_get_max_coloc_per_study_locus_semantic(
        self: TestColocalisationFactory,
        spark: SparkSession,
    ) -> None:
        """Test logic of the function that extracts the maximum log likelihood ratio for each pair of overlapping study-locus."""
        # Prepare mock datasets based on 2 associations
        credset = StudyLocus(
            _df=spark.createDataFrame(
                # 2 associations with a common variant in the locus
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",  # this is a GWAS
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead2",
                        "studyId": "study2",  # this is a eQTL study
                        "locus": [
                            {"variantId": "commonTag", "posteriorProbability": 0.9},
                        ],
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )

        studies = StudyIndex(
            _df=spark.createDataFrame(
                [
                    {
                        "studyId": "study1",
                        "studyType": "gwas",
                        "traitFromSource": "trait1",
                        "projectId": "project1",
                    },
                    {
                        "studyId": "study2",
                        "studyType": "eqtl",
                        "geneId": "gene1",
                        "traitFromSource": "trait2",
                        "projectId": "project2",
                    },
                ]
            ),
            _schema=StudyIndex.get_schema(),
        )
        coloc = Colocalisation(
            _df=spark.createDataFrame(
                [
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 2,
                        "chromosome": "1",
                        "colocalisationMethod": "eCAVIAR",
                        "numberColocalisingVariants": 1,
                        "clpp": 0.81,  # 0.9*0.9
                    }
                ]
            ),
            _schema=Colocalisation.get_schema(),
        )
        expected_coloc_features_df = spark.createDataFrame(
            [
                (1, "gene1", "eqtlColocClppMaximum", 0.81),
                (1, "gene1", "eqtlColocClppMaximumNeighborhood", -4.0),
            ],
            L2GFeature.get_schema(),
        )
        # Test
        coloc_features = ColocalisationFactory._get_max_coloc_per_study_locus(
            credset,
            studies,
            coloc,
            "eCAVIAR",
        )
        assert coloc_features.df.collect() == expected_coloc_features_df.collect()

    def test_get_coloc_features(
        self: TestColocalisationFactory,
        mock_study_locus: StudyLocus,
        mock_study_index: StudyIndex,
        mock_colocalisation: Colocalisation,
    ) -> None:
        """Test the function that calls all the methods to produce colocalisation features."""
        coloc_features = ColocalisationFactory._get_coloc_features(
            mock_study_locus, mock_study_index, mock_colocalisation
        )
        assert isinstance(
            coloc_features, L2GFeature
        ), "Unexpected model type returned from _get_coloc_features"


class TestStudyLocusFactory:
    """Test the StudyLocusFactory methods."""

    def test_get_tss_distance_features(
        self: TestStudyLocusFactory, mock_study_locus: StudyLocus, mock_v2g: V2G
    ) -> None:
        """Test the function that extracts the distance to the TSS."""
        tss_distance = StudyLocusFactory._get_tss_distance_features(
            mock_study_locus, mock_v2g
        )
        assert isinstance(
            tss_distance, L2GFeature
        ), "Unexpected model type returned from _get_tss_distance_features"

    def test_get_vep_features(
        self: TestStudyLocusFactory, mock_study_locus: StudyLocus, mock_v2g: V2G
    ) -> None:
        """Test the function that extracts the VEP features."""
        vep_features = StudyLocusFactory._get_vep_features(mock_study_locus, mock_v2g)
        assert isinstance(
            vep_features, L2GFeature
        ), "Unexpected model type returned from _get_vep_features"
