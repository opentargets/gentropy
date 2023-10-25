"""Collection of methods that extract features from the OTG datasets to be fed in L2G."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from otg.common.spark_helpers import (
    _convert_from_wide_to_long,
    get_record_with_maximum_value,
)
from otg.dataset.l2g.feature_matrix import L2GFeature
from otg.dataset.study_locus import CredibleInterval, StudyLocus

if TYPE_CHECKING:
    from otg.dataset.colocalisation import Colocalisation
    from otg.dataset.study_index import StudyIndex
    from otg.dataset.v2g import V2G


class ColocalisationFactory:
    """Feature extraction in colocalisation."""

    @staticmethod
    def _get_max_coloc_per_study_locus(
        study_locus: StudyLocus,
        studies: StudyIndex,
        colocalisation: Colocalisation,
        colocalisation_method: str,
    ) -> L2GFeature:
        """Get the maximum colocalisation posterior probability for each pair of overlapping study-locus per type of colocalisation method and QTL type.

        Args:
            study_locus (StudyLocus): Study locus dataset
            studies (StudyIndex): Study index dataset
            colocalisation (Colocalisation): Colocalisation dataset
            colocalisation_method (str): Colocalisation method to extract the max from

        Returns:
            L2GFeature: Stores the features with the max coloc probabilities for each pair of study-locus
        """
        # TODO check method is valid
        if colocalisation_method == "COLOC":
            coloc_score_col_name = "log2h4h3"
            coloc_feature_col_template = "max_coloc_llr"

        elif colocalisation_method == "eCAVIAR":
            coloc_score_col_name = "clpp"
            coloc_feature_col_template = "max_coloc_clpp"

        colocalising_study_locus = (
            study_locus.select("studyLocusId", "studyId")
            # annotate studyLoci with overlapping IDs on the left - to just keep GWAS associations
            .join(
                colocalisation._df.selectExpr(
                    "leftStudyLocusId as studyLocusId",
                    "rightStudyLocusId",
                    "colocalisationMethod",
                    f"{coloc_score_col_name} as coloc_score",
                ),
                on="studyLocusId",
                how="inner",
            )
            # bring study metadata to just keep QTL studies on the right
            .join(
                study_locus.df.selectExpr(
                    "studyLocusId as rightStudyLocusId", "studyId as right_studyId"
                ),
                on="rightStudyLocusId",
                how="inner",
            )
            .join(
                f.broadcast(
                    studies._df.selectExpr(
                        "studyId as right_studyId",
                        "studyType as right_studyType",
                        "geneId",
                    )
                ),
                on="right_studyId",
                how="inner",
            )
            .filter(
                (f.col("colocalisationMethod") == colocalisation_method)
                & (f.col("right_studyType") != "gwas")
            )
            .select("studyLocusId", "right_studyType", "geneId", "coloc_score")
        )

        # Max LLR calculation per studyLocus AND type of QTL
        local_max = get_record_with_maximum_value(
            colocalising_study_locus,
            ["studyLocusId", "right_studyType", "geneId"],
            "coloc_score",
        )
        neighbourhood_max = (
            get_record_with_maximum_value(
                colocalising_study_locus,
                ["studyLocusId", "right_studyType"],
                "coloc_score",
            )
            .join(
                local_max.selectExpr("studyLocusId", "coloc_score as coloc_local_max"),
                on="studyLocusId",
                how="inner",
            )
            .withColumn(
                f"{coloc_feature_col_template}_nbh",
                f.col("coloc_local_max") - f.col("coloc_score"),
            )
        )

        # Split feature per molQTL
        local_dfs = []
        nbh_dfs = []
        study_types = (
            colocalising_study_locus.select("right_studyType").distinct().collect()
        )

        for qtl_type in study_types:
            local_max = local_max.filter(
                f.col("right_studyType") == qtl_type
            ).withColumnRenamed(
                "coloc_score", f"{qtl_type}_{coloc_feature_col_template}_local"
            )
            local_dfs.append(local_max)

            neighbourhood_max = neighbourhood_max.filter(
                f.col("right_studyType") == qtl_type
            ).withColumnRenamed(
                f"{coloc_feature_col_template}_nbh",
                f"{qtl_type}_{coloc_feature_col_template}_nbh",
            )
            nbh_dfs.append(neighbourhood_max)

        wide_dfs = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            local_dfs + nbh_dfs,
        )

        return L2GFeature(
            _df=_convert_from_wide_to_long(
                wide_dfs,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=L2GFeature.get_schema(),
        )

    @staticmethod
    def _get_coloc_features(
        study_locus: StudyLocus, studies: StudyIndex, colocalisation: Colocalisation
    ) -> L2GFeature:
        """Calls _get_max_coloc_per_study_locus for both methods and concatenates the results."""
        coloc_llr = ColocalisationFactory._get_max_coloc_per_study_locus(
            study_locus,
            studies,
            colocalisation,
            "COLOC",
        )
        coloc_clpp = ColocalisationFactory._get_max_coloc_per_study_locus(
            study_locus,
            studies,
            colocalisation,
            "eCAVIAR",
        )

        return L2GFeature(
            _df=coloc_llr.df.unionByName(coloc_clpp.df, allowMissingColumns=True),
            _schema=L2GFeature.get_schema(),
        )


class StudyLocusFactory(StudyLocus):
    """Feature extraction in study locus."""

    @staticmethod
    def _get_tss_distance_features(
        study_locus: StudyLocus, distances: V2G
    ) -> L2GFeature:
        """Joins StudyLocus with the V2G to extract the minimum distance to a gene TSS of all variants in a StudyLocus credible set.

        Args:
            study_locus (StudyLocus): Study locus dataset
            distances (V2G): Dataframe containing the distances of all variants to all genes TSS within a region

        Returns:
            L2GFeature: Stores the features with the minimum distance among all variants in the credible set and a gene TSS.

        """
        wide_df = (
            study_locus.filter_credible_set(CredibleInterval.IS95)
            .df.select(
                "studyLocusId",
                "variantId",
                f.explode("locus.variantId").alias("tagVariantId"),
            )
            .join(
                distances.df.selectExpr(
                    "variantId as tagVariantId", "geneId", "distance"
                ),
                on="tagVariantId",
                how="inner",
            )
            .groupBy("studyLocusId", "geneId")
            .agg(
                f.min("distance").alias("distanceTssMinimum"),
                f.mean("distance").alias("distanceTssMean"),
            )
        )

        return L2GFeature(
            _df=_convert_from_wide_to_long(
                wide_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=L2GFeature.get_schema(),
        )
