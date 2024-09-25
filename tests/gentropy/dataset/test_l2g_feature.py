"""Test locus-to-gene feature generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_features.colocalisation import (
    EQtlColocClppMaximumFeature,
    EQtlColocClppMaximumNeighbourhoodFeature,
    EQtlColocH4MaximumFeature,
    EQtlColocH4MaximumNeighbourhoodFeature,
    PQtlColocClppMaximumFeature,
    PQtlColocClppMaximumNeighbourhoodFeature,
    PQtlColocH4MaximumFeature,
    PQtlColocH4MaximumNeighbourhoodFeature,
    SQtlColocClppMaximumFeature,
    SQtlColocClppMaximumNeighbourhoodFeature,
    SQtlColocH4MaximumFeature,
    SQtlColocH4MaximumNeighbourhoodFeature,
    TuQtlColocClppMaximumFeature,
    TuQtlColocClppMaximumNeighbourhoodFeature,
    TuQtlColocH4MaximumFeature,
    TuQtlColocH4MaximumNeighbourhoodFeature,
    common_colocalisation_feature_logic,
    common_neighbourhood_colocalisation_feature_logic,
)
from gentropy.dataset.l2g_features.distance import (
    DistanceFootprintMeanFeature,
    DistanceFootprintMeanNeighbourhoodFeature,
    DistanceFootprintMinimumFeature,
    DistanceFootprintMinimumNeighbourhoodFeature,
    DistanceTssMeanFeature,
    DistanceTssMeanNeighbourhoodFeature,
    DistanceTssMinimumFeature,
    DistanceTssMinimumNeighbourhoodFeature,
    common_distance_feature_logic,
    common_neighbourhood_distance_feature_logic,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.dataset.variant_index import VariantIndex
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.mark.parametrize(
    "feature_class",
    [
        EQtlColocH4MaximumFeature,
        PQtlColocH4MaximumFeature,
        SQtlColocH4MaximumFeature,
        TuQtlColocH4MaximumFeature,
        EQtlColocClppMaximumFeature,
        PQtlColocClppMaximumFeature,
        SQtlColocClppMaximumFeature,
        TuQtlColocClppMaximumFeature,
        EQtlColocClppMaximumNeighbourhoodFeature,
        PQtlColocClppMaximumNeighbourhoodFeature,
        SQtlColocClppMaximumNeighbourhoodFeature,
        TuQtlColocClppMaximumNeighbourhoodFeature,
        EQtlColocH4MaximumNeighbourhoodFeature,
        PQtlColocH4MaximumNeighbourhoodFeature,
        SQtlColocH4MaximumNeighbourhoodFeature,
        TuQtlColocH4MaximumNeighbourhoodFeature,
        DistanceTssMeanFeature,
        DistanceTssMinimumFeature,
        DistanceFootprintMeanFeature,
        DistanceFootprintMinimumFeature,
        DistanceTssMeanNeighbourhoodFeature,
        DistanceTssMinimumNeighbourhoodFeature,
        DistanceFootprintMeanNeighbourhoodFeature,
        DistanceFootprintMinimumNeighbourhoodFeature,
    ],
)
def test_feature_factory_return_type(
    feature_class: Any,
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
    mock_variant_index: VariantIndex,
) -> None:
    """Test that every feature factory returns a L2GFeature dataset."""
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation,
        study_index=mock_study_index,
        variant_index=mock_variant_index,
        study_locus=mock_study_locus,
    )
    feature_dataset = feature_class.compute(
        study_loci_to_annotate=mock_study_locus,
        feature_dependency=loader.get_dependency_by_type(
            feature_class.feature_dependency_type
        ),
    )
    assert isinstance(feature_dataset, L2GFeature)


class TestCommonColocalisationFeatureLogic:
    """Test the common logic of the colocalisation features."""

    def test__common_colocalisation_feature_logic(
        self: TestCommonColocalisationFeatureLogic,
        spark: SparkSession,
    ) -> None:
        """Test the common logic of the colocalisation features.

        The test data associates studyLocusId1 with gene1 based on the colocalisation with studyLocusId2 and studyLocusId3.
        The H4 value of number 2 is higher, therefore the feature value should be based on that.
        """
        feature_name = "eQtlColocH4Maximum"
        observed_df = common_colocalisation_feature_logic(
            self.sample_study_loci_to_annotate,
            self.colocalisation_method,
            self.colocalisation_metric,
            feature_name,
            self.qtl_type,
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_studies,
            study_locus=self.sample_study_locus,
        )
        expected_df = spark.createDataFrame(
            [
                {
                    "studyLocusId": 1,
                    "geneId": "gene1",
                    "eQtlColocH4Maximum": 0.81,
                },
                {
                    "studyLocusId": 1,
                    "geneId": "gene2",
                    "eQtlColocH4Maximum": 0.9,
                },
            ],
        ).select("studyLocusId", "geneId", "eQtlColocH4Maximum")
        assert (
            observed_df.collect() == expected_df.collect()
        ), "The feature values are not as expected."

    def test__common_neighbourhood_colocalisation_feature_logic(
        self: TestCommonColocalisationFeatureLogic, spark: SparkSession
    ) -> None:
        """Test the common logic of the neighbourhood colocalisation features."""
        feature_name = "eQtlColocH4MaximumNeighbourhood"
        observed_df = common_neighbourhood_colocalisation_feature_logic(
            self.sample_study_loci_to_annotate,
            self.colocalisation_method,
            self.colocalisation_metric,
            feature_name,
            self.qtl_type,
            colocalisation=self.sample_colocalisation,
            study_index=self.sample_studies,
            study_locus=self.sample_study_locus,
        )
        expected_df = spark.createDataFrame(
            [
                {
                    "studyLocusId": 1,
                    "geneId": "gene1",
                    "eQtlColocH4MaximumNeighbourhood": 0.08999999999999997,
                },
                {
                    "studyLocusId": 1,
                    "geneId": "gene2",
                    "eQtlColocH4MaximumNeighbourhood": 0.0,
                },
            ],
        ).select("studyLocusId", "geneId", "eQtlColocH4MaximumNeighbourhood")
        assert (
            observed_df.collect() == expected_df.collect()
        ), "The expected and observed dataframes do not match."

    @pytest.fixture(autouse=True)
    def _setup(self: TestCommonColocalisationFeatureLogic, spark: SparkSession) -> None:
        """Set up the test variables."""
        self.colocalisation_method = "Coloc"
        self.colocalisation_metric = "h4"
        self.qtl_type = "eqtl"

        self.sample_study_loci_to_annotate = StudyLocus(
            _df=spark.createDataFrame(
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",  # this is a GWAS
                        "chromosome": "1",
                    },
                ]
            ),
            _schema=StudyLocus.get_schema(),
        )
        self.sample_colocalisation = Colocalisation(
            _df=spark.createDataFrame(
                [
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 2,
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.81,
                    },
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 3,  # qtl linked to the same gene as studyLocusId 2 with a lower score
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.50,
                    },
                    {
                        "leftStudyLocusId": 1,
                        "rightStudyLocusId": 4,  # qtl linked to a diff gene and with the highest score
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.90,
                    },
                ],
                schema=Colocalisation.get_schema(),
            ),
            _schema=Colocalisation.get_schema(),
        )
        self.sample_study_locus = StudyLocus(
            _df=spark.createDataFrame(
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",  # this is a GWAS
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 2,
                        "variantId": "lead1",
                        "studyId": "study2",  # this is a QTL (same gee)
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 3,
                        "variantId": "lead1",
                        "studyId": "study3",  # this is another QTL (same gene)
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": 4,
                        "variantId": "lead1",
                        "studyId": "study4",  # this is another QTL (diff gene)
                        "chromosome": "1",
                    },
                ]
            ),
            _schema=StudyLocus.get_schema(),
        )
        self.sample_studies = StudyIndex(
            _df=spark.createDataFrame(
                [
                    {
                        "studyId": "study1",
                        "studyType": "gwas",
                        "geneId": None,
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
                    {
                        "studyId": "study3",
                        "studyType": "eqtl",
                        "geneId": "gene1",
                        "traitFromSource": "trait3",
                        "projectId": "project3",
                    },
                    {
                        "studyId": "study4",
                        "studyType": "eqtl",
                        "geneId": "gene2",
                        "traitFromSource": "trait4",
                        "projectId": "project4",
                    },
                ]
            ),
            _schema=StudyIndex.get_schema(),
        )


class TestCommonDistanceFeatureLogic:
    """Test the CommonDistanceFeatureLogic methods."""

    @pytest.mark.parametrize(
        ("feature_name", "expected_distance"),
        [
            ("distanceTssMinimum", 2.5),
            ("distanceTssMean", 3.75),
        ],
    )
    def test_common_distance_feature_logic(
        self: TestCommonDistanceFeatureLogic,
        spark: SparkSession,
        feature_name: str,
        expected_distance: int,
    ) -> None:
        """Test the logic of the function that extracts the distance between the variants in a credible set and a gene."""
        agg_expr = (
            f.min(f.col("weightedDistance"))
            if feature_name == "distanceTssMinimum"
            else f.mean(f.col("weightedDistance"))
        )
        observed_df = common_distance_feature_logic(
            self.sample_study_locus,
            variant_index=self.sample_variant_index,
            feature_name=feature_name,
            distance_type=self.distance_type,
            agg_expr=agg_expr,
        )
        assert observed_df.first()[feature_name] == expected_distance

    def test_common_neighbourhood_colocalisation_feature_logic(
        self: TestCommonDistanceFeatureLogic,
        spark: SparkSession,
    ) -> None:
        """Test the logic of the function that extracts the distance between the variants in a credible set and the nearby genes."""
        another_sample_variant_index = VariantIndex(
            _df=spark.createDataFrame(
                [
                    (
                        "lead1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {"distanceFromTss": 10, "targetId": "gene1"},
                            {"distanceFromTss": 100, "targetId": "gene2"},
                        ],
                    ),
                    (
                        "tag1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {"distanceFromTss": 5, "targetId": "gene1"},
                        ],
                    ),
                ],
                self.variant_index_schema,
            ),
            _schema=VariantIndex.get_schema(),
        )
        observed_df = common_neighbourhood_distance_feature_logic(
            self.sample_study_locus,
            variant_index=another_sample_variant_index,
            feature_name="distanceTssMinimum",
            distance_type=self.distance_type,
            agg_expr=f.min("weightedDistance"),
        ).orderBy(f.col("distanceTssMinimum").asc())
        expected_df = spark.createDataFrame(
            ([1, "gene2", -47.5], [1, "gene1", 0.0]),
            ["studyLocusId", "geneId", "distanceTssMinimum"],
        ).orderBy(f.col("distanceTssMinimum").asc())
        assert (
            observed_df.collect() == expected_df.collect()
        ), "Output doesn't meet the expectation."

    @pytest.fixture(autouse=True)
    def _setup(self: TestCommonDistanceFeatureLogic, spark: SparkSession) -> None:
        """Set up testing fixtures."""
        self.distance_type = "distanceFromTss"
        self.sample_study_locus = StudyLocus(
            _df=spark.createDataFrame(
                [
                    {
                        "studyLocusId": 1,
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {
                                "variantId": "lead1",
                                "posteriorProbability": 0.5,
                            },
                            {
                                "variantId": "tag1",  # this variant is closer to gene1
                                "posteriorProbability": 0.5,
                            },
                        ],
                        "chromosome": "1",
                    },
                ],
                StudyLocus.get_schema(),
            ),
            _schema=StudyLocus.get_schema(),
        )
        self.variant_index_schema = StructType(
            [
                StructField("variantId", StringType(), True),
                StructField("chromosome", StringType(), True),
                StructField("position", IntegerType(), True),
                StructField("referenceAllele", StringType(), True),
                StructField("alternateAllele", StringType(), True),
                StructField(
                    "transcriptConsequences",
                    ArrayType(
                        StructType(
                            [
                                StructField("distanceFromTss", LongType(), True),
                                StructField("targetId", StringType(), True),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        self.sample_variant_index = VariantIndex(
            _df=spark.createDataFrame(
                [
                    (
                        "lead1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {"distanceFromTss": 10, "targetId": "gene1"},
                        ],
                    ),
                    (
                        "tag1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {"distanceFromTss": 5, "targetId": "gene1"},
                        ],
                    ),
                ],
                self.variant_index_schema,
            ),
            _schema=VariantIndex.get_schema(),
        )
