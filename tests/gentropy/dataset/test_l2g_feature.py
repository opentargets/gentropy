"""Test locus-to-gene feature generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from gentropy.dataset.colocalisation import Colocalisation
from gentropy.dataset.gene_index import GeneIndex
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
    DistanceSentinelFootprintFeature,
    DistanceSentinelFootprintNeighbourhoodFeature,
    DistanceSentinelTssFeature,
    DistanceSentinelTssNeighbourhoodFeature,
    DistanceTssMeanFeature,
    DistanceTssMeanNeighbourhoodFeature,
    common_distance_feature_logic,
    common_neighbourhood_distance_feature_logic,
)
from gentropy.dataset.l2g_features.l2g_feature import L2GFeature
from gentropy.dataset.l2g_features.vep import (
    VepMaximumFeature,
    VepMaximumNeighbourhoodFeature,
    VepMeanFeature,
    VepMeanNeighbourhoodFeature,
    common_neighbourhood_vep_feature_logic,
    common_vep_feature_logic,
)
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
        DistanceTssMeanNeighbourhoodFeature,
        DistanceFootprintMeanFeature,
        DistanceFootprintMeanNeighbourhoodFeature,
        DistanceSentinelTssFeature,
        DistanceSentinelTssNeighbourhoodFeature,
        DistanceSentinelFootprintFeature,
        DistanceSentinelFootprintNeighbourhoodFeature,
        VepMaximumFeature,
        VepMeanFeature,
        VepMaximumNeighbourhoodFeature,
        VepMeanNeighbourhoodFeature,
    ],
)
def test_feature_factory_return_type(
    feature_class: Any,
    mock_study_locus: StudyLocus,
    mock_colocalisation: Colocalisation,
    mock_study_index: StudyIndex,
    mock_variant_index: VariantIndex,
    mock_gene_index: GeneIndex,
) -> None:
    """Test that every feature factory returns a L2GFeature dataset."""
    loader = L2GFeatureInputLoader(
        colocalisation=mock_colocalisation,
        study_index=mock_study_index,
        variant_index=mock_variant_index,
        study_locus=mock_study_locus,
        gene_index=mock_gene_index,
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
                    "studyLocusId": "1",
                    "geneId": "gene1",
                    "eQtlColocH4Maximum": 0.81,
                },
                {
                    "studyLocusId": "1",
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
                    "studyLocusId": "1",
                    "geneId": "gene1",
                    "eQtlColocH4MaximumNeighbourhood": 0.08999999999999997,
                },
                {
                    "studyLocusId": "1",
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
                        "studyLocusId": "1",
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
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "2",
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.81,
                        "rightStudyType": "eqtl",
                    },
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "3",  # qtl linked to the same gene as studyLocusId 2 with a lower score
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.50,
                        "rightStudyType": "eqtl",
                    },
                    {
                        "leftStudyLocusId": "1",
                        "rightStudyLocusId": "4",  # qtl linked to a diff gene and with the highest score
                        "chromosome": "1",
                        "colocalisationMethod": "COLOC",
                        "numberColocalisingVariants": 1,
                        "h4": 0.90,
                        "rightStudyType": "eqtl",
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
                        "studyLocusId": "1",
                        "variantId": "lead1",
                        "studyId": "study1",  # this is a GWAS
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": "2",
                        "variantId": "lead1",
                        "studyId": "study2",  # this is a QTL (same gee)
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": "3",
                        "variantId": "lead1",
                        "studyId": "study3",  # this is another QTL (same gene)
                        "chromosome": "1",
                    },
                    {
                        "studyLocusId": "4",
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
        ("feature_name", "expected_data"),
        [
            (
                "distanceSentinelTss",
                [
                    {
                        "studyLocusId": "1",
                        "geneId": "gene1",
                        "distanceSentinelTss": 0.0,
                    },
                    {
                        "studyLocusId": "1",
                        "geneId": "gene2",
                        "distanceSentinelTss": 0.95,
                    },
                ],
            ),
            (
                "distanceTssMean",
                [
                    {"studyLocusId": "1", "geneId": "gene1", "distanceTssMean": 0.09},
                    {"studyLocusId": "1", "geneId": "gene2", "distanceTssMean": 0.65},
                ],
            ),
        ],
    )
    def test_common_distance_feature_logic(
        self: TestCommonDistanceFeatureLogic,
        spark: SparkSession,
        feature_name: str,
        expected_data: dict[str, Any],
    ) -> None:
        """Test the logic of the function that extracts features from distance.

        2 tests:
        - distanceSentinelTss: distance of the sentinel is 10, the max distance is 10. In log scale, the score is 0.
        - distanceTssMean: avg distance of any variant in the credible set, weighted by its posterior.
        """
        observed_df = (
            common_distance_feature_logic(
                self.sample_study_locus,
                variant_index=self.sample_variant_index,
                feature_name=feature_name,
                distance_type=self.distance_type,
                genomic_window=10,
            )
            .withColumn(feature_name, f.round(f.col(feature_name), 2))
            .orderBy(feature_name)
        )
        expected_df = (
            spark.createDataFrame(expected_data)
            .select("studyLocusId", "geneId", feature_name)
            .orderBy(feature_name)
        )
        assert (
            observed_df.collect() == expected_df.collect()
        ), f"Expected and observed dataframes are not equal for feature {feature_name}."

    def test_common_neighbourhood_colocalisation_feature_logic(
        self: TestCommonDistanceFeatureLogic,
        spark: SparkSession,
    ) -> None:
        """Test the logic of the function that extracts the distance between the sentinel of a credible set and the nearby genes."""
        feature_name = "distanceSentinelTssNeighbourhood"
        observed_df = (
            common_neighbourhood_distance_feature_logic(
                self.sample_study_locus,
                variant_index=self.sample_variant_index,
                feature_name=feature_name,
                distance_type=self.distance_type,
                genomic_window=10,
            )
            .withColumn(feature_name, f.round(f.col(feature_name), 2))
            .orderBy(f.col(feature_name).asc())
        )
        expected_df = spark.createDataFrame(
            (["1", "gene1", -0.48], ["1", "gene2", 0.48]),
            ["studyLocusId", "geneId", feature_name],
        ).orderBy(feature_name)
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
                        "studyLocusId": "1",
                        "variantId": "lead1",
                        "studyId": "study1",
                        "locus": [
                            {
                                "variantId": "lead1",
                                "posteriorProbability": 0.5,
                            },
                            {
                                "variantId": "tag1",
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
                                StructField("isEnsemblCanonical", BooleanType(), True),
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
                            {
                                "distanceFromTss": 10,
                                "targetId": "gene1",
                                "isEnsemblCanonical": True,
                            },
                            {
                                "distanceFromTss": 2,
                                "targetId": "gene2",
                                "isEnsemblCanonical": True,
                            },
                        ],
                    ),
                    (
                        "tag1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {
                                "distanceFromTss": 5,
                                "targetId": "gene1",
                                "isEnsemblCanonical": True,
                            },
                        ],
                    ),
                ],
                self.variant_index_schema,
            ),
            _schema=VariantIndex.get_schema(),
        )


class TestCommonVepFeatureLogic:
    """Test the common_vep_feature_logic methods."""

    @pytest.mark.parametrize(
        ("feature_name", "expected_data"),
        [
            (
                "vepMean",
                [
                    {
                        "studyLocusId": "1",
                        "geneId": "gene1",
                        "vepMean": 0.33,
                    },
                    {
                        "studyLocusId": "1",
                        "geneId": "gene2",
                        "vepMean": 0.5,
                    },
                ],
            ),
            (
                "vepMaximum",
                [
                    {
                        "studyLocusId": "1",
                        "geneId": "gene1",
                        "vepMaximum": 0.66,
                    },
                    {
                        "studyLocusId": "1",
                        "geneId": "gene2",
                        "vepMaximum": 1.0,
                    },
                ],
            ),
        ],
    )
    def test_common_vep_feature_logic(
        self: TestCommonVepFeatureLogic,
        spark: SparkSession,
        feature_name: str,
        expected_data: dict[str, Any],
    ) -> None:
        """Test the logic of the function that extracts features from VEP's functional consequences."""
        observed_df = (
            common_vep_feature_logic(
                self.sample_study_locus,
                variant_index=self.sample_variant_index,
                feature_name=feature_name,
            )
            .withColumn(feature_name, f.round(f.col(feature_name), 2))
            .orderBy(feature_name)
        )
        expected_df = (
            spark.createDataFrame(expected_data)
            .select("studyLocusId", "geneId", feature_name)
            .orderBy(feature_name)
        )
        assert (
            observed_df.collect() == expected_df.collect()
        ), f"Expected and observed dataframes are not equal for feature {feature_name}."

    def test_common_neighbourhood_vep_feature_logic_no_protein_coding(
        self: TestCommonVepFeatureLogic,
        spark: SparkSession,
    ) -> None:
        """Test the logic of the function that extracts the maximum severity score for a gene given the average of the maximum scores for all protein coding genes in the vicinity.

        Because the genes in the vicinity are all non coding, the neighbourhood features should equal the local ones.
        """
        feature_name = "vepMaximumNeighbourhood"
        sample_gene_index = GeneIndex(
            _df=spark.createDataFrame(
                [
                    {
                        "geneId": "gene1",
                        "biotype": "lncRNA",
                        "chromosome": "1",
                    },
                    {
                        "geneId": "gene2",
                        "biotype": "lncRNA",
                        "chromosome": "1",
                    },
                ],
                GeneIndex.get_schema(),
            ),
            _schema=GeneIndex.get_schema(),
        )
        observed_df = (
            common_neighbourhood_vep_feature_logic(
                self.sample_study_locus,
                variant_index=self.sample_variant_index,
                gene_index=sample_gene_index,
                feature_name=feature_name,
            )
            .withColumn(feature_name, f.round(f.col(feature_name), 2))
            .orderBy(f.col(feature_name).asc())
            .select("studyLocusId", "geneId", feature_name)
        )
        expected_df = (
            spark.createDataFrame(
                (["1", "gene1", 0.66], ["1", "gene2", 1.0]),
                ["studyLocusId", "geneId", feature_name],
            )
            .orderBy(feature_name)
            .select("studyLocusId", "geneId", feature_name)
        )
        assert (
            observed_df.collect() == expected_df.collect()
        ), "Output doesn't meet the expectation."

        def test_common_neighbourhood_vep_feature_logic(
            self: TestCommonVepFeatureLogic,
            spark: SparkSession,
        ) -> None:
            """Test the logic of the function that extracts the maximum severity score for a gene given the average of the maximum scores for all protein coding genes in the vicinity."""
            feature_name = "vepMaximumNeighbourhood"
            sample_gene_index = GeneIndex(
                _df=spark.createDataFrame(
                    [
                        {
                            "geneId": "gene1",
                            "biotype": "protein_coding",
                            "chromosome": "1",
                        },
                        {
                            "geneId": "gene2",
                            "biotype": "lncRNA",
                            "chromosome": "1",
                        },
                    ],
                    GeneIndex.get_schema(),
                ),
                _schema=GeneIndex.get_schema(),
            )
            observed_df = (
                common_neighbourhood_vep_feature_logic(
                    self.sample_study_locus,
                    variant_index=self.sample_variant_index,
                    gene_index=sample_gene_index,
                    feature_name=feature_name,
                )
                .withColumn(feature_name, f.round(f.col(feature_name), 2))
                .orderBy(f.col(feature_name).asc())
            )
            expected_df = (
                spark.createDataFrame(
                    (["1", "gene1", 0.0], ["1", "gene2", 0.34]),
                    ["studyLocusId", "geneId", feature_name],
                )
                .select("studyLocusId", "geneId", feature_name)
                .orderBy(feature_name)
            )
            assert (
                observed_df.collect() == expected_df.collect()
            ), "Output doesn't meet the expectation."

    @pytest.fixture(autouse=True)
    def _setup(self: TestCommonVepFeatureLogic, spark: SparkSession) -> None:
        """Set up testing fixtures."""
        self.sample_study_locus = StudyLocus(
            _df=spark.createDataFrame(
                [
                    {
                        "studyLocusId": "1",
                        "variantId": "var1",
                        "studyId": "study1",
                        "locus": [
                            {
                                "variantId": "var1",
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
        self.sample_variant_index = VariantIndex(
            _df=spark.createDataFrame(
                [
                    (
                        "var1",
                        "chrom",
                        1,
                        "A",
                        "T",
                        [
                            {
                                "targetId": "gene1",
                                "variantFunctionalConsequenceIds": [
                                    "SO_0001630",  # splice_region_variant (0.33)
                                    "SO_0001822",  # inframe_deletion (0.66)
                                ],
                                "isEnsemblCanonical": True,
                            },
                            {
                                "targetId": "gene2",
                                "variantFunctionalConsequenceIds": [
                                    "SO_0001589",  # frameshift_variant (1.0)
                                ],
                                "isEnsemblCanonical": True,
                            },
                        ],
                    ),
                ],
                schema=StructType(
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
                                        StructField("targetId", StringType(), True),
                                        StructField(
                                            "isEnsemblCanonical", BooleanType(), True
                                        ),
                                        StructField(
                                            "variantFunctionalConsequenceIds",
                                            ArrayType(StringType(), True),
                                            True,
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
            ),
            _schema=VariantIndex.get_schema(),
        )
