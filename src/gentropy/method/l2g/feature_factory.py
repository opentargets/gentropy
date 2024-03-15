"""Collection of methods that extract features from the gentropy datasets to be fed in L2G."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.spark_helpers import (
    convert_from_wide_to_long,
    get_record_with_maximum_value,
)
from gentropy.dataset.l2g_feature import L2GFeature
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.v2g import V2G


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

        Raises:
            ValueError: If the colocalisation method is not supported
        """
        if colocalisation_method not in ["COLOC", "eCAVIAR"]:
            raise ValueError(
                f"Colocalisation method {colocalisation_method} not supported"
            )
        if colocalisation_method == "COLOC":
            coloc_score_col_name = "log2h4h3"
            coloc_feature_col_template = "ColocLlrMaximum"

        elif colocalisation_method == "eCAVIAR":
            coloc_score_col_name = "clpp"
            coloc_feature_col_template = "ColocClppMaximum"

        colocalising_study_locus = (
            study_locus.df.select("studyLocusId", "studyId")
            # annotate studyLoci with overlapping IDs on the left - to just keep GWAS associations
            .join(
                colocalisation.df.selectExpr(
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
                    studies.df.selectExpr(
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

        # Max PP calculation per studyLocus AND type of QTL
        local_max = get_record_with_maximum_value(
            colocalising_study_locus,
            ["studyLocusId", "right_studyType", "geneId"],
            "coloc_score",
        )

        intercept = 0.0001
        neighbourhood_max = (
            local_max.selectExpr(
                "studyLocusId", "coloc_score as coloc_local_max", "geneId"
            )
            .join(
                # Add maximum in the neighborhood
                get_record_with_maximum_value(
                    colocalising_study_locus.withColumnRenamed(
                        "coloc_score", "coloc_neighborhood_max"
                    ),
                    ["studyLocusId", "right_studyType"],
                    "coloc_neighborhood_max",
                ).drop("geneId"),
                on="studyLocusId",
            )
            .withColumn(
                f"{coloc_feature_col_template}Neighborhood",
                f.log10(
                    f.abs(
                        f.col("coloc_local_max")
                        - f.col("coloc_neighborhood_max")
                        + f.lit(intercept)
                    )
                ),
            )
        ).drop("coloc_neighborhood_max", "coloc_local_max")

        # Split feature per molQTL
        local_dfs = []
        nbh_dfs = []
        qtl_types: list[str] = (
            colocalising_study_locus.select("right_studyType")
            .distinct()
            .toPandas()["right_studyType"]
            .tolist()
        )
        for qtl_type in qtl_types:
            filtered_local_max = (
                local_max.filter(f.col("right_studyType") == qtl_type)
                .withColumnRenamed(
                    "coloc_score",
                    f"{qtl_type}{coloc_feature_col_template}",
                )
                .drop("right_studyType")
            )
            local_dfs.append(filtered_local_max)

            filtered_neighbourhood_max = (
                neighbourhood_max.filter(f.col("right_studyType") == qtl_type)
                .withColumnRenamed(
                    f"{coloc_feature_col_template}Neighborhood",
                    f"{qtl_type}{coloc_feature_col_template}Neighborhood",
                )
                .drop("right_studyType")
            )
            nbh_dfs.append(filtered_neighbourhood_max)

        wide_dfs = reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            local_dfs + nbh_dfs,
        )

        return L2GFeature(
            _df=convert_from_wide_to_long(
                wide_dfs.groupBy("studyLocusId", "geneId").agg(
                    *(
                        f.first(f.col(c), ignorenulls=True).alias(c)
                        for c in wide_dfs.columns
                        if c
                        not in [
                            "studyLocusId",
                            "geneId",
                        ]
                    )
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
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
            _df=convert_from_wide_to_long(
                wide_df,
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ),
            _schema=L2GFeature.get_schema(),
        )

    @staticmethod
    def _get_vep_features(
        credible_set: StudyLocus,
        v2g: V2G,
    ) -> L2GFeature:
        """Get the maximum VEP score for all variants in a locus's 95% credible set.

        This informs about functional impact of the variants in the locus. For more information on variant consequences, see: https://www.ensembl.org/info/genome/variation/prediction/predicted_data.html
        Two metrics: max VEP score per study locus and gene, and max VEP score per study locus.


        Args:
            credible_set (StudyLocus): Study locus dataset with the associations to be annotated
            v2g (V2G): V2G dataset with the variant/gene relationships and their consequences

        Returns:
            L2GFeature: Stores the features with the max VEP score.
        """

        def _aggregate_vep_feature(
            df: DataFrame,
            aggregation_expr: Column,
            aggregation_cols: list[str],
            feature_name: str,
        ) -> DataFrame:
            """Extracts the maximum or average VEP score after grouping by the given columns. Different aggregations return different predictive annotations.

            If the group_cols include "geneId", the maximum/mean VEP score per gene is returned.
            Otherwise, the maximum/mean VEP score for all genes in the neighborhood of the locus is returned.

            Args:
                df (DataFrame): DataFrame with the VEP scores for each variant in a studyLocus
                aggregation_expr (Column): Aggregation expression to apply
                aggregation_cols (list[str]): Columns to group by
                feature_name (str): Name of the feature to be returned

            Returns:
                DataFrame: DataFrame with the maximum VEP score per locus or per locus/gene
            """
            if "geneId" in aggregation_cols:
                return df.groupBy(aggregation_cols).agg(
                    aggregation_expr.alias(feature_name)
                )
            return (
                df.groupBy(aggregation_cols)
                .agg(
                    aggregation_expr.alias(feature_name),
                    f.collect_set("geneId").alias("geneId"),
                )
                .withColumn("geneId", f.explode("geneId"))
            )

        credible_set_w_variant_consequences = (
            credible_set.filter_credible_set(CredibleInterval.IS95)
            .df.withColumn("variantInLocus", f.explode_outer("locus"))
            .select(
                f.col("studyLocusId"),
                f.col("variantId"),
                f.col("studyId"),
                f.col("variantInLocus.variantId").alias("variantInLocusId"),
                f.col("variantInLocus.posteriorProbability").alias(
                    "variantInLocusPosteriorProbability"
                ),
            )
            .join(
                # Join with V2G to get variant consequences
                v2g.df.filter(f.col("datasourceId") == "variantConsequence").selectExpr(
                    "variantId as variantInLocusId", "geneId", "score"
                ),
                on="variantInLocusId",
            )
            .select(
                "studyLocusId",
                "variantId",
                "studyId",
                "geneId",
                "score",
                (f.col("score") * f.col("variantInLocusPosteriorProbability")).alias(
                    "weightedScore"
                ),
            )
            .distinct()
        )

        return L2GFeature(
            _df=convert_from_wide_to_long(
                reduce(
                    lambda x, y: x.unionByName(y, allowMissingColumns=True),
                    [
                        # Calculate overall max VEP score for all genes in the vicinity
                        credible_set_w_variant_consequences.transform(
                            _aggregate_vep_feature,
                            f.max("score"),
                            ["studyLocusId"],
                            "vepMaximumNeighborhood",
                        ),
                        # Calculate overall max VEP score per gene
                        credible_set_w_variant_consequences.transform(
                            _aggregate_vep_feature,
                            f.max("score"),
                            ["studyLocusId", "geneId"],
                            "vepMaximum",
                        ),
                        # Calculate mean VEP score for all genes in the vicinity
                        credible_set_w_variant_consequences.transform(
                            _aggregate_vep_feature,
                            f.mean("weightedScore"),
                            ["studyLocusId"],
                            "vepMeanNeighborhood",
                        ),
                        # Calculate mean VEP score per gene
                        credible_set_w_variant_consequences.transform(
                            _aggregate_vep_feature,
                            f.mean("weightedScore"),
                            ["studyLocusId", "geneId"],
                            "vepMean",
                        ),
                    ],
                ),
                id_vars=("studyLocusId", "geneId"),
                var_name="featureName",
                value_name="featureValue",
            ).filter(f.col("featureValue").isNotNull()),
            _schema=L2GFeature.get_schema(),
        )
