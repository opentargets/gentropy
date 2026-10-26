"""Test L2G feature matrix methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.credible_set import CredibleSetLocus
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.target_index import TargetIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def test_select_features_inheritance(mock_l2g_feature_matrix: L2GFeatureMatrix) -> None:
    """Test L2GFeatureMatrix.select_features method inherits the instance attributes in the new instance."""
    new_instance = mock_l2g_feature_matrix.select_features(
        features_list=["distanceTssMean"]
    )
    assert new_instance.features_list == ["distanceTssMean"]
    # Because the feature matrix contains the gold standard flag information, the new fixed colums should be the same
    assert "goldStandardSet" in new_instance.fixed_cols


class TestFromFeaturesList:
    """Test L2GFeatureMatrix.from_features_list method.

    If the columns from the features list are there, it means that the business logic is working (the dataframe is not empty when converting from long to wide).
    """

    def test_study_locus(
        self: TestFromFeaturesList,
    ) -> None:
        """Test building feature matrix for a SL with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum", "geneCount500kb"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
            target_index=self.sample_target_index,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_study_locus, features_list, loader
        )
        for feature in features_list:
            assert feature in fm._df.columns, (
                f"Feature {feature} not found in feature matrix."
            )

    def test_append_missing_columns_no_null(
        self: TestFromFeaturesList,
    ) -> None:
        """Test appending feature matrix when there are NO null columns wanted by feature_list."""
        features_list = ["eQtlColocH4Maximum", "geneCount500kb"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
            target_index=self.sample_target_index,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_study_locus, features_list, loader
        ).append_null_features(features_list)
        for feature in features_list:
            assert feature in fm._df.columns, (
                f"Feature {feature} not found in feature matrix."
            )

    def test_append_missing_columns_null(
        self: TestFromFeaturesList,
    ) -> None:
        """Test appending feature matrix when there ARE null columns wanted by feature_list."""
        features_list = ["eQtlColocH4Maximum", "geneCount500kb", "pQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
            target_index=self.sample_target_index,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_study_locus, features_list, loader
        ).append_null_features(features_list)
        for feature in features_list:
            assert feature in fm._df.columns, (
                f"Feature {feature} not found in feature matrix."
            )
            assert feature in fm.features_list, (
                f"Feature {feature} not found in feature matrix features list."
            )

    def test_features_without_rows_are_null_columns(
        self: TestFromFeaturesList,
    ) -> None:
        """Every requested feature is a column, sorted by name, even when it has no rows."""
        features_list = ["geneCount500kb", "pQtlColocH4Maximum", "eQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
            target_index=self.sample_target_index,
        )
        fm = L2GFeatureMatrix.from_features_list(
            self.sample_study_locus, features_list, loader
        )
        assert fm.features_list == sorted(features_list)
        assert fm._df.filter(f.col("pQtlColocH4Maximum").isNotNull()).count() == 0

    def test_study_locus_incorrect_feature_name(
        self: TestFromFeaturesList,
    ) -> None:
        """Test appending feature matrix when there ARE null columns wanted by feature_list."""
        features_list = ["eQtlColocH4Maximum", "geneCount500kb", "foo"]
        loader = L2GFeatureInputLoader(
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_study_index,
            study_locus=self.sample_study_locus,
            target_index=self.sample_target_index,
        )
        with pytest.raises(ValueError) as excinfo:
            L2GFeatureMatrix.from_features_list(
                self.sample_study_locus, features_list, loader
            )
        assert "Feature foo not found." in str(excinfo.value)

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
            assert feature in fm._df.columns, (
                f"Feature {feature} not found in feature matrix."
            )

    @pytest.fixture(autouse=True)
    def _setup(self: TestFromFeaturesList, spark: SparkSession) -> None:
        """Setup fixture."""
        self.sample_gold_standard = L2GGoldStandard(
            _df=spark.createDataFrame(
                [(1, "var1", "gwas1", "g1", "efo1", "positive", ["a_source"])],
                L2GGoldStandard.get_schema(),
            ),
            _schema=L2GGoldStandard.get_schema(),
        )
        self.sample_study_locus = CredibleSetLocus(
            _df=spark.createDataFrame(
                [
                    (
                        "1",
                        "var1",
                        "gwas1",
                        "X",
                        2,
                        [
                            {"variantId": "var1", "posteriorProbability": 0.8},
                            {"variantId": "var12", "posteriorProbability": 0.2},
                        ],
                        False,
                    ),
                    (
                        "2",
                        "var2",
                        "eqtl1",
                        "X",
                        10,
                        [
                            {"variantId": "var2", "posteriorProbability": 1.0},
                        ],
                        False,
                    ),
                ],
                schema=StructType(
                    [
                        StructField("credibleSetId", StringType(), True),
                        StructField("variantId", StringType(), True),
                        StructField("studyId", StringType(), True),
                        StructField("chromosome", StringType(), True),
                        StructField("position", IntegerType(), True),
                        StructField(
                            "locus",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("variantId", StringType(), True),
                                        StructField(
                                            "posteriorProbability", FloatType(), True
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                        StructField("isTransQtl", BooleanType(), True),
                    ]
                ),
            ),
            _schema=CredibleSetLocus.get_schema(),
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
                [("1", "2", "eqtl", "X", "COLOC", 1, 0.9)],
                [
                    "leftLocusId",
                    "rightLocusId",
                    "rightStudyType",
                    "chromosome",
                    "colocalisationMethod",
                    "numberColocalisingVariants",
                    "h4",
                ],
            ),
            _schema=Colocalisation.get_schema(),
        )
        self.sample_target_index = TargetIndex(
            _df=spark.createDataFrame(
                [
                    {
                        "id": "g1",
                        "genomicLocation": {
                            "chromosome": "X",
                        },
                        "tss": 200,
                        "biotype": "protein_coding",
                    },
                    {
                        "id": "g2",
                        "genomicLocation": {
                            "chromosome": "X",
                        },
                        "tss": 300,
                        "biotype": "protein_coding",
                    },
                ],
                TargetIndex.get_schema(),
            ),
            _schema=TargetIndex.get_schema(),
        )


def test_fill_na(spark: SparkSession) -> None:
    """Tests L2GFeatureMatrix.fill_na, particularly the imputation logic."""
    sample_fm = L2GFeatureMatrix(
        _df=spark.createDataFrame(
            [
                {
                    "credibleSetId": "1",
                    "geneId": "gene1",
                    "proteinGeneCount500kb": 3.0,
                    "geneCount500kb": 8.0,
                    "isProteinCoding": 1.0,
                    "anotherFeature": None,
                },
                {
                    "credibleSetId": "1",
                    "geneId": "gene2",
                    "proteinGeneCount500kb": 4.0,
                    "geneCount500kb": 10.0,
                    "isProteinCoding": 1.0,
                    "anotherFeature": None,
                },
                {
                    "credibleSetId": "1",
                    "geneId": "gene3",
                    "proteinGeneCount500kb": None,
                    "geneCount500kb": None,
                    "isProteinCoding": None,
                    "anotherFeature": None,
                },
            ],
            schema="credibleSetId STRING, geneId STRING, proteinGeneCount500kb DOUBLE, geneCount500kb DOUBLE, isProteinCoding DOUBLE, anotherFeature DOUBLE",
        ),
    )
    observed_df = sample_fm.fill_na()._df.filter(f.col("geneId") == "gene3")
    expected_df_missing_row = spark.createDataFrame(
        [
            {
                "credibleSetId": "1",
                "geneId": "gene3",
                "proteinGeneCount500kb": 3.5,
                "geneCount500kb": 9.0,
                "isProteinCoding": 0.0,
                "anotherFeature": 0.0,
            },
        ],
    ).select(
        "credibleSetId",
        "geneId",
        "proteinGeneCount500kb",
        "geneCount500kb",
        "isProteinCoding",
        "anotherFeature",
    )
    assert observed_df.collect() == expected_df_missing_row.collect()


class TestCredibleSetLocusBuildFeatureMatrix:
    """Collection of tests related to building feature matrix from credible set loci."""

    def test_build_feature_matrix(
        self: TestCredibleSetLocusBuildFeatureMatrix,
        mock_credible_set_locus: CredibleSetLocus,
        mock_colocalisation: Colocalisation,
        mock_study_index: StudyIndex,
    ) -> None:
        """Test building feature matrix with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=mock_colocalisation,
            study_index=mock_study_index,
            study_locus=mock_credible_set_locus,
        )
        fm = mock_credible_set_locus.build_feature_matrix(features_list, loader)
        assert isinstance(fm, L2GFeatureMatrix), (
            "Feature matrix should be of type L2GFeatureMatrix"
        )

    def test_build_feature_matrix_append_null_false(
        self: TestCredibleSetLocusBuildFeatureMatrix,
        mock_credible_set_locus: CredibleSetLocus,
        mock_colocalisation: Colocalisation,
        mock_study_index_no_pqtl: StudyIndex,
    ) -> None:
        """Test building feature matrix with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum", "pQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=mock_colocalisation,
            study_index=mock_study_index_no_pqtl,
            study_locus=mock_credible_set_locus,
        )

        fm = mock_credible_set_locus.build_feature_matrix(
            features_list, loader, append_null_features=False
        )

        assert isinstance(fm, L2GFeatureMatrix), (
            "Feature matrix should be of type L2GFeatureMatrix"
        )
        assert "eQtlColocH4Maximum" in fm._df.columns
        assert "eQtlColocH4Maximum" in fm.features_list
        assert "pQtlColocH4Maximum" not in fm._df.columns
        assert "pQtlColocH4Maximum" not in fm.features_list

    def test_build_feature_matrix_append_null_true(
        self: TestCredibleSetLocusBuildFeatureMatrix,
        mock_credible_set_locus: CredibleSetLocus,
        mock_colocalisation: Colocalisation,
        mock_study_index_no_pqtl: StudyIndex,
    ) -> None:
        """Test building feature matrix with the eQtlColocH4Maximum feature."""
        features_list = ["eQtlColocH4Maximum", "pQtlColocH4Maximum"]
        loader = L2GFeatureInputLoader(
            colocalisation=mock_colocalisation,
            study_index=mock_study_index_no_pqtl,
            study_locus=mock_credible_set_locus,
        )
        fm = mock_credible_set_locus.build_feature_matrix(
            features_list, loader, append_null_features=True
        )
        assert isinstance(fm, L2GFeatureMatrix), (
            "Feature matrix should be of type L2GFeatureMatrix"
        )
        assert "eQtlColocH4Maximum" in fm._df.columns
        assert "eQtlColocH4Maximum" in fm.features_list
        assert "pQtlColocH4Maximum" in fm._df.columns
        assert "pQtlColocH4Maximum" in fm.features_list
