"""Performing linkage disequilibrium (LD) operations."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from otg.dataset.ld_index import LDIndex


class LDAnnotator:
    """Class to annotate linkage disequilibrium (LD) operations from GnomAD."""

    @staticmethod
    def _calculate_weighted_r_overall(ld_set: Column) -> Column:
        """Aggregation of weighted R information using ancestry proportions."""
        return f.transform(
            ld_set,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                # r2Overall is the accumulated sum of each r2 relative to the population size
                f.aggregate(
                    x["rValues"],
                    f.lit(0.0),
                    lambda acc, y: acc
                    + f.coalesce(
                        f.pow(y["r"], 2) * y["relativeSampleSize"], f.lit(0.0)
                    ),  # we use coalesce to avoid problems when r/relativeSampleSize is null
                ).alias("r2Overall"),
            ),
        )

    @staticmethod
    def _add_population_size(ld_set: Column, study_populations: Column) -> Column:
        """Add population size to each rValues entry in the ldSet.

        Args:
            ld_set (Column): LD set
            study_populations (Column): Study populations

        Returns:
            Column: LD set with added 'relativeSampleSize' field
        """
        # Create a population to relativeSampleSize map from the struct
        populations_map = f.map_from_arrays(
            study_populations["ldPopulation"],
            study_populations["relativeSampleSize"],
        )
        return f.transform(
            ld_set,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                f.transform(
                    x["rValues"],
                    lambda y: f.struct(
                        y["population"].alias("population"),
                        y["r"].alias("r"),
                        populations_map[y["population"]].alias("relativeSampleSize"),
                    ),
                ).alias("rValues"),
            ),
        )

    @classmethod
    def annotate_variants_with_ld(
        cls: type[LDAnnotator], variants_df: DataFrame, ld_index: LDIndex
    ) -> DataFrame:
        """Annotate linkage disequilibrium (LD) information to a set of variants.

        Args:
            variants_df (DataFrame): Input DataFrame with a `variantId` column containing variant IDs (hg38)
            ld_index (LDIndex): LD index

        Returns:
            DataFrame: DataFrame with LD annotations
        """
        return variants_df.join(ld_index.df, on=["variantId", "chromosome"], how="left")

    @classmethod
    def annotate_associations_with_ld(
        cls: type[LDAnnotator],
        associations_df: DataFrame,
        ld_index: LDIndex,
    ) -> DataFrame:
        """Annotate linkage disequilibrium (LD) information to a set of associations.

        We first join the associations dataframe with the LD index. Then, we add the population size of the study to each rValues entry in the ldSet to calculate the relative r between lead/tag for that study.
        Finally, we aggregate the weighted R information using ancestry proportions.

        Args:
            associations_df (DataFrame): Study locus DataFrame with a `ldPopulationsStructure` column containing population structure information
            ld_index (LDIndex): Dataset with LD information for every variant present in gnomAD LD matrix

        Returns:
            DataFrame: Following the same schema as the input DataFrame, but with LD annotations (`ldSet` column)
        """
        return (
            # Bring LD information
            associations_df.join(
                ld_index.df, on=["variantId", "chromosome"], how="left"
            )
            # Add population size to each rValues entry in the ldSet
            .withColumn(
                "ldSet",
                cls._add_population_size(
                    f.col("ldSet"), f.col("ldPopulationStructure")
                ),
            )
            # Aggregate weighted R information using ancestry proportions
            .withColumn(
                "ldSet",
                cls._calculate_weighted_r_overall(f.col("ldSet")),
            ).drop("ldPopulationStructure")
        )
