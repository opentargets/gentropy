"""Test L2G feature matrix methods."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature_matrix import L2GFeatureMatrix
from gentropy.dataset.l2g_gold_standard import L2GGoldStandard
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
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
        self.sample_study_locus = StudyLocus(
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
                        StructField("studyLocusId", StringType(), True),
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
                                            "posteriorProbability", DoubleType(), True
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
                [("1", "2", "eqtl", "X", "COLOC", 1, 0.9)],
                [
                    "leftStudyLocusId",
                    "rightStudyLocusId",
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


class TestFilterDarkMatterLoci:
    """Tests for L2GFeatureMatrix.filter_dark_matter_loci."""

    # Minimal schema: signal feature + nearest feature + gold standard label
    _SCHEMA = (
        "studyLocusId STRING, geneId STRING, goldStandardSet STRING, "
        "eQtlColocClppMaximum FLOAT, distanceSentinelTssNeighbourhood FLOAT"
    )

    def _make_fm(self, spark: SparkSession, rows: list[Any]) -> L2GFeatureMatrix:
        """Build a minimal gold-standard-annotated L2GFeatureMatrix from row tuples."""
        return L2GFeatureMatrix(
            _df=spark.createDataFrame(rows, schema=self._SCHEMA),
            with_gold_standard=True,
        )

    def test_all_dark_matter_locus_removed(self, spark: SparkSession) -> None:
        """Locus where every positive is dark matter (no signal, not nearest) is dropped entirely."""
        fm = self._make_fm(
            spark,
            [
                # dark matter positive: no signal, not nearest
                ("loc1", "gene1", "positive", 0.0, 0.5),
                # negative in the same locus — should also be dropped
                ("loc1", "gene2", "negative", 0.0, 1.0),
                # clean locus — should be kept
                ("loc2", "gene3", "positive", 0.9, 0.5),
                ("loc2", "gene4", "negative", 0.0, 1.0),
            ],
        )
        filtered, stats = fm.filter_dark_matter_loci()
        remaining = {
            r.studyLocusId for r in filtered._df.select("studyLocusId").collect()
        }
        assert "loc1" not in remaining
        assert "loc2" in remaining
        assert stats["dark_matter"]["study_locus_ids_removed"] == 1

    def test_mixed_locus_kept(self, spark: SparkSession) -> None:
        """Locus with one dark matter positive and one signal positive is kept."""
        fm = self._make_fm(
            spark,
            [
                ("loc1", "gene1", "positive", 0.0, 0.5),  # dark matter
                ("loc1", "gene2", "positive", 0.8, 0.5),  # has signal → locus kept
                ("loc1", "gene3", "negative", 0.0, 1.0),
            ],
        )
        filtered, stats = fm.filter_dark_matter_loci()
        remaining = {
            r.studyLocusId for r in filtered._df.select("studyLocusId").collect()
        }
        assert "loc1" in remaining
        assert stats["dark_matter"]["study_locus_ids_removed"] == 0

    def test_nearest_gene_positive_protected(self, spark: SparkSession) -> None:
        """Positive with no signal but nearest-gene status (neighbourhood = 1.0) is protected."""
        fm = self._make_fm(
            spark,
            [
                # nearest gene (neighbourhood = 1.0), no signal → NOT dark matter
                ("loc1", "gene1", "positive", 0.0, 1.0),
                ("loc1", "gene2", "negative", 0.0, 0.3),
            ],
        )
        filtered, stats = fm.filter_dark_matter_loci()
        remaining = {
            r.studyLocusId for r in filtered._df.select("studyLocusId").collect()
        }
        assert "loc1" in remaining
        assert stats["dark_matter"]["study_locus_ids_removed"] == 0

    def test_no_nearest_features_is_noop(self, spark: SparkSession) -> None:
        """When no neighbourhood distance features are present the filter is a no-op."""
        fm = L2GFeatureMatrix(
            _df=spark.createDataFrame(
                [("loc1", "gene1", "positive", 0.0)],
                "studyLocusId STRING, geneId STRING, goldStandardSet STRING, eQtlColocClppMaximum FLOAT",
            ),
            with_gold_standard=True,
        )
        filtered, stats = fm.filter_dark_matter_loci()
        assert filtered._df.count() == 1
        assert stats == {}

    def test_null_signal_treated_as_no_signal(self, spark: SparkSession) -> None:
        """NULL signal features (from missing coloc/VEP inner joins) are treated as zero."""
        fm = self._make_fm(
            spark,
            [
                # NULL signal (inner join produced no row) + not nearest → dark matter
                ("loc1", "gene1", "positive", None, 0.5),
                ("loc1", "gene2", "negative", None, 1.0),
            ],
        )
        filtered, stats = fm.filter_dark_matter_loci()
        remaining = {
            r.studyLocusId for r in filtered._df.select("studyLocusId").collect()
        }
        assert "loc1" not in remaining
        assert stats["dark_matter"]["study_locus_ids_removed"] == 1

    def test_null_vep_treated_as_no_signal(self, spark: SparkSession) -> None:
        """NULL vepMaximum (absent VEP annotation) is coalesced to 0.0 and treated as below threshold."""
        fm = L2GFeatureMatrix(
            _df=spark.createDataFrame(
                [
                    # zero QTL signal, NULL vepMaximum, not nearest → dark matter
                    ("loc1", "gene1", "positive", 0.0, 0.5, None),
                    ("loc1", "gene2", "negative", 0.0, 1.0, None),
                ],
                "studyLocusId STRING, geneId STRING, goldStandardSet STRING, "
                "eQtlColocClppMaximum FLOAT, distanceSentinelTssNeighbourhood FLOAT, "
                "vepMaximum FLOAT",
            ),
            with_gold_standard=True,
        )
        filtered, stats = fm.filter_dark_matter_loci()
        remaining = {
            r.studyLocusId for r in filtered._df.select("studyLocusId").collect()
        }
        assert "loc1" not in remaining
        assert stats["dark_matter"]["study_locus_ids_removed"] == 1

    def test_raises_without_gold_standard(self, spark: SparkSession) -> None:
        """Calling the filter on a matrix without gold standard labels raises ValueError."""
        fm = L2GFeatureMatrix(
            _df=spark.createDataFrame(
                [("loc1", "gene1", 0.0)],
                "studyLocusId STRING, geneId STRING, eQtlColocClppMaximum FLOAT",
            ),
            with_gold_standard=False,
        )
        with pytest.raises(ValueError, match="gold standard"):
            fm.filter_dark_matter_loci()


def test_fill_na(spark: SparkSession) -> None:
    """Tests L2GFeatureMatrix.fill_na, particularly the imputation logic."""
    sample_fm = L2GFeatureMatrix(
        _df=spark.createDataFrame(
            [
                {
                    "studyLocusId": "1",
                    "geneId": "gene1",
                    "proteinGeneCount500kb": 3.0,
                    "geneCount500kb": 8.0,
                    "isProteinCoding": 1.0,
                    "anotherFeature": None,
                },
                {
                    "studyLocusId": "1",
                    "geneId": "gene2",
                    "proteinGeneCount500kb": 4.0,
                    "geneCount500kb": 10.0,
                    "isProteinCoding": 1.0,
                    "anotherFeature": None,
                },
                {
                    "studyLocusId": "1",
                    "geneId": "gene3",
                    "proteinGeneCount500kb": None,
                    "geneCount500kb": None,
                    "isProteinCoding": None,
                    "anotherFeature": None,
                },
            ],
            schema="studyLocusId STRING, geneId STRING, proteinGeneCount500kb DOUBLE, geneCount500kb DOUBLE, isProteinCoding DOUBLE, anotherFeature DOUBLE",
        ),
    )
    observed_df = sample_fm.fill_na()._df.filter(f.col("geneId") == "gene3")
    expected_df_missing_row = spark.createDataFrame(
        [
            {
                "studyLocusId": "1",
                "geneId": "gene3",
                "proteinGeneCount500kb": 3.5,
                "geneCount500kb": 9.0,
                "isProteinCoding": 0.0,
                "anotherFeature": 0.0,
            },
        ],
    ).select(
        "studyLocusId",
        "geneId",
        "proteinGeneCount500kb",
        "geneCount500kb",
        "isProteinCoding",
        "anotherFeature",
    )
    assert observed_df.collect() == expected_df_missing_row.collect()
