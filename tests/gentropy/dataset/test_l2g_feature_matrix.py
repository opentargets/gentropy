"""Test L2G feature matrix methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class TestFromFeaturesList:
    """Test L2GFeatureMatrix.from_features_list method.

    If the columns from the features list are there, it means that the business logic is working (the dataframe is not empty when converting from long to wide).
    """

    def test_study_locus(
        self: TestFromFeaturesList,
    ) -> None:
        """Test building feature matrix for a SL with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_study_locus, features_list, loader
        )
        for feature in features_list:
            assert (
                feature in fm._df.columns
            ), f"Feature {feature} not found in feature matrix."

    def test_gold_standard(
        self: TestFromFeaturesList,
    ) -> None:
        """Test building feature matrix for a gold standard with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_gold_standard, features_list, loader
        )
        for feature in features_list:
            assert (
                feature in fm._df.columns
            ), f"Feature {feature} not found in feature matrix."

    @pytest.fixture(autouse=True)
    def _setup(self: TestFromFeaturesList, spark: SparkSession) -> None:
        """Setup fixture."""
        self.sample_gold_standard = L2GGoldStandard(
            _df=spark.createDataFrame(
                [(1, "var1", "gwas1", "g1", "positive", ["a_source"])],
                L2GGoldStandard.get_schema(),
            ),
            _schema=L2GGoldStandard.get_schema(),
        )
        self.sample_study_locus = StudyLocus(
            _df=spark.createDataFrame(
                [
                    (
                        "1",
                        "var1",
                        "gwas1",
                        [
                            {"variantId": "var1", "posteriorProbability": 0.8},
                            {"variantId": "var12", "posteriorProbability": 0.2},
                        ],
                    ),
                    (
                        "2",
                        "var2",
                        "eqtl1",
                        [
                            {"variantId": "var2", "posteriorProbability": 1.0},
                        ],
                    ),
                ],
                schema=StructType(
                    [
                        StructField("studyLocusId", LongType(), True),
                        StructField("variantId", StringType(), True),
                        StructField("studyId", StringType(), True),
                        StructField(
                            "locus",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("variantId", StringType(), True),
                                        StructField(
                                            "posteriorProbability", DoubleType(), True
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
            ),
            _schema=StudyLocus.get_schema(),
        )
        self.sample_study_index = StudyIndex(
            _df=spark.createDataFrame(
                [("gwas1", "gwas", None, "p1"), ("eqtl1", "eqtl", "g1", "p2")],
                [
                    "studyId",
                    "studyType",
                    "geneId",
                    "projectId",
                ],
            ),
            _schema=StudyIndex.get_schema(),
        )
        self.sample_colocalisation = Colocalisation(
            _df=spark.createDataFrame(
                [("1", "2", "X", "COLOC", 1, 0.9)],
                [
                    "leftStudyLocusId",
                    "rightStudyLocusId",
                    "chromosome",
                    "colocalisationMethod",
                    "numberColocalisingVariants",
                    "h4",
                ],
            ),
            _schema=Colocalisation.get_schema(),
        )
