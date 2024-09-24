"""Test locus-to-gene feature generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.l2g_feature import (
    DistanceFootprintMeanFeature,
    DistanceFootprintMeanNeighbourhoodFeature,
    DistanceFootprintMinimumFeature,
    DistanceFootprintMinimumNeighbourhoodFeature,
    DistanceTssMeanFeature,
    DistanceTssMeanNeighbourhoodFeature,
    DistanceTssMinimumFeature,
    DistanceTssMinimumNeighbourhoodFeature,
    EQtlColocClppMaximumFeature,
    EQtlColocClppMaximumNeighbourhoodFeature,
    EQtlColocH4MaximumFeature,
    EQtlColocH4MaximumNeighbourhoodFeature,
    L2GFeature,
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
    _common_colocalisation_feature_logic,
    _common_neighbourhood_colocalisation_feature_logic,
)
from gentropy.dataset.study_index import StudyIndex
from gentropy.dataset.study_locus import StudyLocus
from gentropy.method.l2g.feature_factory import L2GFeatureInputLoader

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from gentropy.dataset.variant_index import VariantIndex


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
        observed_df = _common_colocalisation_feature_logic(
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
        observed_df = _common_neighbourhood_colocalisation_feature_logic(
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


# class TestColocalisationFactory:
#     """Test the ColocalisationFactory methods."""
#     def test_get_max_coloc_per_credible_set_semantic(
#         self: TestColocalisationFactory,
#         spark: SparkSession,
#     ) -> None:
#         """Test logic of the function that extracts the maximum log likelihood ratio for each pair of overlapping study-locus."""
#         # Prepare mock datasets based on 2 associations
#         credset = StudyLocus(
#             _df=spark.createDataFrame(
#                 # 2 associations with a common variant in the locus
#                 [
#                     {
#                         "studyLocusId": 1,
#                         "variantId": "lead1",
#                         "studyId": "study1",  # this is a GWAS
#                         "locus": [
#                             {"variantId": "commonTag", "posteriorProbability": 0.9},
#                         ],
#                         "chromosome": "1",
#                     },
#                     {
#                         "studyLocusId": 2,
#                         "variantId": "lead2",
#                         "studyId": "study2",  # this is a eQTL study
#                         "locus": [
#                             {"variantId": "commonTag", "posteriorProbability": 0.9},
#                         ],
#                         "chromosome": "1",
#                     },
#                 ],
#                 StudyLocus.get_schema(),
#             ),
#             _schema=StudyLocus.get_schema(),
#         )

#         studies = StudyIndex(
#             _df=spark.createDataFrame(
#                 [
#                     {
#                         "studyId": "study1",
#                         "studyType": "gwas",
#                         "traitFromSource": "trait1",
#                         "projectId": "project1",
#                     },
#                     {
#                         "studyId": "study2",
#                         "studyType": "eqtl",
#                         "geneId": "gene1",
#                         "traitFromSource": "trait2",
#                         "projectId": "project2",
#                     },
#                 ]
#             ),
#             _schema=StudyIndex.get_schema(),
#         )
#         coloc = Colocalisation(
#             _df=spark.createDataFrame(
#                 [
#                     {
#                         "leftStudyLocusId": 1,
#                         "rightStudyLocusId": 2,
#                         "chromosome": "1",
#                         "colocalisationMethod": "eCAVIAR",
#                         "numberColocalisingVariants": 1,
#                         "clpp": 0.81,  # 0.9*0.9
#                         "log2h4h3": None,
#                     }
#                 ],
#                 schema=Colocalisation.get_schema(),
#             ),
#             _schema=Colocalisation.get_schema(),
#         )
#         expected_coloc_features_df = spark.createDataFrame(
#             [
#                 (1, "gene1", "eqtlColocClppMaximum", 0.81),
#                 (1, "gene1", "eqtlColocClppMaximumNeighborhood", -4.0),
#             ],
#             L2GFeature.get_schema(),
#         )
#         # Test
#         coloc_features = ColocalisationFactory._get_max_coloc_per_credible_set(
#             coloc,
#             credset,
#             studies,
#         )
#         assert coloc_features.df.collect() == expected_coloc_features_df.collect()
