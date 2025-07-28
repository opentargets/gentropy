"""Performing linkage disequilibrium (LD) operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.spark import order_array_of_structs_by_field
from gentropy.dataset.study_locus import StudyLocus, StudyLocusQualityCheck

if TYPE_CHECKING:
    from pyspark.sql import Column

    from gentropy.dataset.ld_index import LDIndex
    from gentropy.dataset.study_index import StudyIndex


class LDAnnotator:
    """Class to annotate linkage disequilibrium (LD) operations from GnomAD."""

    @staticmethod
    def _get_major_population(ordered_populations: Column) -> Column:
        """Get major population based on an ldPopulationStructure array ordered by relativeSampleSize.

        If there is a tie for the major population, nfe is selected if it is one of the major populations.
        The first population in the array is selected if there is no tie for the major population, or there is a tie but nfe is not one of the major populations.

        Args:
            ordered_populations (Column): ldPopulationStructure array ordered by relativeSampleSize

        Returns:
            Column: major population
        """
        major_population_size = ordered_populations["relativeSampleSize"][0]
        major_populations = f.filter(
            ordered_populations,
            lambda x: x["relativeSampleSize"] == major_population_size,
        )
        # Check if nfe (Non-Finnish European) is one of the major populations
        has_nfe = f.filter(major_populations, lambda x: x["ldPopulation"] == "nfe")
        return f.when(
            (f.size(major_populations) > 1) & (f.size(has_nfe) == 1), f.lit("nfe")
        ).otherwise(ordered_populations["ldPopulation"][0])

    @staticmethod
    def _calculate_r2_major(ld_set: Column, major_population: Column) -> Column:
        """Calculate R2 using R of the major population in the study.

        Args:
            ld_set (Column): LD set
            major_population (Column): Major population of the study

        Returns:
            Column: LD set with added 'r2Overall' field
        """
        ld_set_with_major_pop = f.transform(
            ld_set,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                f.filter(
                    x["rValues"], lambda y: y["population"] == major_population
                ).alias("rValues"),
            ),
        )
        return f.transform(
            ld_set_with_major_pop,
            lambda x: f.struct(
                x["tagVariantId"].alias("tagVariantId"),
                f.coalesce(f.pow(x["rValues"]["r"][0], 2), f.lit(0.0)).alias(
                    "r2Overall"
                ),
            ),
        )

    @staticmethod
    def _qc_unresolved_ld(ld_set: Column, quality_controls: Column) -> Column:
        """Flag associations with unresolved LD.

        Args:
            ld_set (Column): LD set
            quality_controls (Column): Quality controls

        Returns:
            Column: Quality controls with added 'UNRESOLVED_LD' field
        """
        return StudyLocus.update_quality_flag(
            quality_controls,
            ld_set.isNull(),
            StudyLocusQualityCheck.UNRESOLVED_LD,
        )

    @staticmethod
    def _rescue_lead_variant(ld_set: Column, variant_id: Column) -> Column:
        """Rescue lead variant.

        In cases in which no LD information is available but a lead variant is available, we include the lead as the only variant in the ldSet.

        Args:
            ld_set (Column): LD set
            variant_id (Column): Variant ID

        Returns:
            Column: LD set with added 'tagVariantId' field
        """
        return f.when(
            ((ld_set.isNull() | (f.size(ld_set) == 0)) & variant_id.isNotNull()),
            f.array(
                f.struct(
                    variant_id.alias("tagVariantId"),
                    f.lit(1).alias("r2Overall"),
                )
            ),
        ).otherwise(ld_set)

    @classmethod
    def ld_annotate(
        cls: type[LDAnnotator],
        associations: StudyLocus,
        studies: StudyIndex,
        ld_index: LDIndex,
        r2_threshold: float = 0.5,
    ) -> StudyLocus:
        """Annotate linkage disequilibrium (LD) information to a set of studyLocus.

        This function:
            1. Annotates study locus with population structure information ordered by relativeSampleSize from the study index
            2. Joins the LD index to the StudyLocus
            3. Gets the major population from the population structure
            4. Calculates R2 by using the R of the major ancestry
            5. Flags associations with variants that are not found in the LD reference
            6. Rescues lead variant when no LD information is available but lead variant is available

        !!! note
            Because the LD index has a pre-set threshold of R2 = 0.5, this is the minimum threshold for the LD information to be included in the ldSet.

        Args:
            associations (StudyLocus): Dataset to be LD annotated
            studies (StudyIndex): Dataset with study information
            ld_index (LDIndex): Dataset with LD information for every variant present in LD matrix
            r2_threshold (float): R2 threshold to filter the LD set on. Default is 0.5.

        Returns:
            StudyLocus: including additional column with LD information.
        """
        return StudyLocus(
            _df=(
                associations.df
                # Drop ldSet column if already available
                .select(*[col for col in associations.df.columns if col != "ldSet"])
                # Annotate study locus with population structure ordered by relativeSampleSize from study index
                .join(
                    studies.df.select(
                        "studyId",
                        order_array_of_structs_by_field(
                            "ldPopulationStructure", "relativeSampleSize"
                        ).alias("ldPopulationStructure"),
                    ),
                    on="studyId",
                    how="left",
                )
                # Bring LD information from LD Index
                .join(
                    ld_index.df,
                    on=["variantId", "chromosome"],
                    how="left",
                )
                # Get major population from population structure if population structure available
                .withColumn(
                    "majorPopulation",
                    f.when(
                        f.col("ldPopulationStructure").isNotNull(),
                        cls._get_major_population(f.col("ldPopulationStructure")),
                    ),
                )
                # Calculate R2 using R of the major population
                .withColumn(
                    "ldSet",
                    f.when(
                        f.col("ldPopulationStructure").isNotNull(),
                        cls._calculate_r2_major(
                            f.col("ldSet"), f.col("majorPopulation")
                        ),
                    ),
                )
                .drop("ldPopulationStructure", "majorPopulation")
                # Filter the LD set by the R2 threshold and set to null if no LD information passes the threshold
                .withColumn(
                    "ldSet",
                    StudyLocus.filter_ld_set(f.col("ldSet"), r2_threshold),
                )
                .withColumn("ldSet", f.when(f.size("ldSet") > 0, f.col("ldSet")))
                # QC: Flag associations with variants that are not found in the LD reference
                .withColumn(
                    "qualityControls",
                    cls._qc_unresolved_ld(f.col("ldSet"), f.col("qualityControls")),
                )
                # Add lead variant to empty ldSet when no LD information is available but lead variant is available
                .withColumn(
                    "ldSet",
                    cls._rescue_lead_variant(f.col("ldSet"), f.col("variantId")),
                )
                # Ensure that the lead varaitn is always with r2==1
                .withColumn(
                    "ldSet",
                    f.expr(
                        """
                        transform(ldSet, x ->
                            IF(x.tagVariantId == variantId,
                                named_struct('tagVariantId', x.tagVariantId, 'r2Overall', 1.0),
                                x
                            )
                        )
                        """
                    ),
                )
            ),
            _schema=StudyLocus.get_schema(),
        )._qc_no_population()
