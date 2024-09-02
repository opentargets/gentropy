"""Collection of methods that extract features from the gentropy datasets to be fed in L2G."""

from __future__ import annotations

from functools import reduce
from itertools import chain
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.spark_helpers import (
    convert_from_wide_to_long,
    get_record_with_maximum_value,
)
from gentropy.dataset.l2g_feature import L2GFeature
from gentropy.dataset.study_locus import CredibleInterval, StudyLocus
from gentropy.method.colocalisation import Coloc, ECaviar

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.dataset.colocalisation import Colocalisation
    from gentropy.dataset.study_index import StudyIndex
    from gentropy.dataset.v2g import V2G


class ColocalisationFactory:
    """Feature extraction in colocalisation."""

    @classmethod
    def _add_colocalisation_metric(cls: type[ColocalisationFactory]) -> Column:
        """Expression that adds a `colocalisationMetric` column to the colocalisation dataframe in preparation for feature extraction.

        Returns:
            Column: The expression that adds a `colocalisationMetric` column with the derived metric
        """
        method_metric_map = {
            ECaviar.METHOD_NAME: ECaviar.METHOD_METRIC,
            Coloc.METHOD_NAME: Coloc.METHOD_METRIC,
        }
        map_expr = f.create_map(*[f.lit(x) for x in chain(*method_metric_map.items())])
        return map_expr[f.col("colocalisationMethod")].alias("colocalisationMetric")

    @staticmethod
    def _get_max_coloc_per_credible_set(
        colocalisation: Colocalisation,
        credible_set: StudyLocus,
        studies: StudyIndex,
    ) -> L2GFeature:
        """Get the maximum colocalisation posterior probability for each pair of overlapping study-locus per type of colocalisation method and QTL type.

        Args:
            colocalisation (Colocalisation): Colocalisation dataset
            credible_set (StudyLocus): Study locus dataset
            studies (StudyIndex): Study index dataset

        Returns:
            L2GFeature: Stores the features with the max coloc probabilities for each pair of study-locus
        """
        colocalisation_df = colocalisation.df.select(
            f.col("leftStudyLocusId").alias("studyLocusId"),
            "rightStudyLocusId",
            f.coalesce("h4", "clpp").alias("score"),
            ColocalisationFactory._add_colocalisation_metric(),
        )

        colocalising_credible_sets = (
            credible_set.df.select("studyLocusId", "studyId")
            # annotate studyLoci with overlapping IDs on the left - to just keep GWAS associations
            .join(
                colocalisation_df,
                on="studyLocusId",
                how="inner",
            )
            # bring study metadata to just keep QTL studies on the right
            .join(
                credible_set.df.join(
                    studies.df.select("studyId", "studyType", "geneId"), "studyId"
                ).selectExpr(
                    "studyLocusId as rightStudyLocusId",
                    "studyType as right_studyType",
                    "geneId",
                ),
                on="rightStudyLocusId",
                how="inner",
            )
            .filter(f.col("right_studyType") != "gwas")
            .select(
                "studyLocusId",
                "right_studyType",
                "geneId",
                "score",
                "colocalisationMetric",
            )
        )

        # Max PP calculation per credible set AND type of QTL AND colocalisation method
        local_max = (
            get_record_with_maximum_value(
                colocalising_credible_sets,
                ["studyLocusId", "right_studyType", "geneId", "colocalisationMetric"],
                "score",
            )
            .select(
                "*",
                f.col("score").alias("max_score"),
                f.lit("Local").alias("score_type"),
            )
            .drop("score")
        )

        neighbourhood_max = (
            local_max.selectExpr(
                "studyLocusId", "max_score as local_max_score", "geneId"
            )
            .join(
                # Add maximum in the neighborhood
                get_record_with_maximum_value(
                    colocalising_credible_sets.withColumnRenamed(
                        "score", "tmp_nbh_max_score"
                    ),
                    ["studyLocusId", "right_studyType", "colocalisationMetric"],
                    "tmp_nbh_max_score",
                ).drop("geneId"),
                on="studyLocusId",
            )
            .withColumn("score_type", f.lit("Neighborhood"))
            .withColumn(
                "max_score",
                f.log10(
                    f.abs(
                        f.col("local_max_score")
                        - f.col("tmp_nbh_max_score")
                        + f.lit(0.0001)  # intercept
                    )
                ),
            )
        ).drop("tmp_nbh_max_score", "local_max_score")

        return L2GFeature(
            _df=(
                # Combine local and neighborhood metrics
                local_max.unionByName(
                    neighbourhood_max, allowMissingColumns=True
                ).select(
                    "studyLocusId",
                    "geneId",
                    # Feature name is a concatenation of the QTL type, colocalisation metric and if it's local or in the vicinity
                    f.concat_ws(
                        "",
                        f.col("right_studyType"),
                        f.lit("Coloc"),
                        f.initcap(f.col("colocalisationMetric")),
                        f.lit("Maximum"),
                        f.regexp_replace(f.col("score_type"), "Local", ""),
                    ).alias("featureName"),
                    f.col("max_score").cast("float").alias("featureValue"),
                )
            ),
            _schema=L2GFeature.get_schema(),
        )


class StudyLocusFactory(StudyLocus):
    """Feature extraction in study locus."""

    @staticmethod
    def _get_tss_distance_features(credible_set: StudyLocus, v2g: V2G) -> L2GFeature:
        """Joins StudyLocus with the V2G to extract a score that is based on the distance to a gene TSS of any variant weighted by its posterior probability in a credible set.

        Args:
            credible_set (StudyLocus): Credible set dataset
            v2g (V2G): Dataframe containing the distances of all variants to all genes TSS within a region

        Returns:
            L2GFeature: Stores the features with the score of weighting the distance to the TSS by the posterior probability of the variant

        """
        wide_df = (
            credible_set.filter_credible_set(CredibleInterval.IS95)
            .df.withColumn("variantInLocus", f.explode_outer("locus"))
            .select(
                "studyLocusId",
                "variantId",
                f.col("variantInLocus.variantId").alias("variantInLocusId"),
                f.col("variantInLocus.posteriorProbability").alias(
                    "variantInLocusPosteriorProbability"
                ),
            )
            .join(
                v2g.df.filter(f.col("datasourceId") == "canonical_tss").selectExpr(
                    "variantId as variantInLocusId", "geneId", "score"
                ),
                on="variantInLocusId",
                how="inner",
            )
            .withColumn(
                "weightedScore",
                f.col("score") * f.col("variantInLocusPosteriorProbability"),
            )
            .groupBy("studyLocusId", "geneId")
            .agg(
                f.min("weightedScore").alias("distanceTssMinimum"),
                f.mean("weightedScore").alias("distanceTssMean"),
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
                            f.max("weightedScore"),
                            ["studyLocusId"],
                            "vepMaximumNeighborhood",
                        ),
                        # Calculate overall max VEP score per gene
                        credible_set_w_variant_consequences.transform(
                            _aggregate_vep_feature,
                            f.max("weightedScore"),
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
