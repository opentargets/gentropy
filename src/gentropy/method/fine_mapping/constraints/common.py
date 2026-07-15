from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window

from gentropy.common.ld_population import LDPopulation
from gentropy.dataset.study_index import (
    StudyAnalysisFlag,
    StudyIndex,
    StudyQualityCheck,
)
from gentropy.method.fine_mapping.constraints.model import MethodConstraint


class IsGwasStudyType(MethodConstraint):
    """Class representing the constraint for the MultiSuSuSiE method."""

    def apply(self, df: DataFrame) -> DataFrame:
        """Mark the rows of the dataframe that satisfy the constraint.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        return df.filter(f.col("studyType") == "gwas")


class HasSumstats(MethodConstraint):
    """Class representing the constraint for the MultiSuSuSiE method."""

    def apply(self, df: DataFrame) -> DataFrame:
        """Mark the rows of the dataframe that satisfy the constraint.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        return df.filter(f.col("hasSumstats"))


class HasAllowedAncestry(MethodConstraint):
    """Class representing the constraint for single & allowed ancestry for SuSiE based methods."""

    def __init__(
        self,
        allowed_ancestries: list[LDPopulation],
        relative_sample_size_threshold: float,
        multi_ancestry: bool,
    ):
        self.allowed_ancestries = allowed_ancestries
        self.relative_sample_size_threshold = relative_sample_size_threshold
        self.multi_ancestry = multi_ancestry

    @staticmethod
    def _merge(prev_ancestry: Column, current_ancestry: Column) -> Column:
        return f.when(
            prev_ancestry.getField("relativeSampleSize")
            >= current_ancestry.getField("relativeSampleSize"),
            prev_ancestry,
        ).otherwise(current_ancestry)

    def apply(self, df: DataFrame) -> DataFrame:
        """Mark the rows of the dataframe that satisfy the constraint.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        ess = (4 * f.col("nCases") * f.col("nControls")) / (
            f.col("nCases") + f.col("nControls")
        )
        # Conditions for valid sampleSize calculation
        cc_match_n = f.col("nCases") + f.col("nControls") == f.col("nSamples")
        cases_above_zero = f.col("nCases") > 0
        controls_above_zero = f.col("nControls") > 0
        cases_null_or_zero = (f.col("nCases").isNull()) | (f.col("nCases") == 0)
        controls_null_or_zero = (f.col("nControls").isNull()) | (
            f.col("nControls") == 0
        )
        n_samples_above_zero = f.col("nSamples") > 0
        valid_sample_size = (
            f.when(cases_above_zero & controls_above_zero & cc_match_n, ess)
            .when(
                (cases_null_or_zero & controls_null_or_zero & n_samples_above_zero),
                f.col("nSamples").cast("double"),
            )
            .otherwise(f.lit(None))
        )

        i = f.col("ldPopulationStructure").getItem(0)
        major_anc = f.reduce(f.col("ldPopulationStructure"), i, merge=self._merge)
        trait_set = f.concat_ws(
            ",", f.sort_array(f.col("traitFromSourceMappedIds"), asc=True)
        )
        common_trait_set_studies = (
            Window()
            .partitionBy(
                f.col("traitSet"), f.col("majorAncestry").getField("ldPopulation")
            )
            .orderBy(f.col("validSampleSize").desc())
        )

        multi_ancestry_for_trait = Window().partitionBy(f.col("traitSet"))

        # Apply filter to remove studies with invalid sampleSize,
        # disallowed ancestries and studies with relative sampleSize below threshold.
        df = (
            df.withColumn("validSampleSize", valid_sample_size)
            .filter(f.col("validSampleSize").isNotNull())
            .withColumn("majorAncestry", major_anc)
            .withColumn("traitSet", trait_set)
            .filter(
                f.col("majorAncestry").getField("relativeSampleSize")
                >= self.relative_sample_size_threshold
            )
            .filter(
                f.col("majorAncestry")
                .getField("ldPopulation")
                .isin([population.value for population in self.allowed_ancestries])
            )
        )

        # Apply filter to keep only representative studies &
        # the ones that have multiple ancestries in allowed set.
        if self.multi_ancestry:
            df = (
                df.withColumn(
                    "representativeStudy",
                    f.row_number().over(common_trait_set_studies) == 1,
                )
                .filter(f.col("representativeStudy"))
                .withColumn(
                    "traitHasMultipleAncestries",
                    f.countDistinct(
                        f.col("majorAncestry").getField("ldPopulation")
                    ).over(multi_ancestry_for_trait)
                    > 1,
                )
                .filter(f.col("traitHasMultipleAncestries"))
            )

        return df


class PassSumstatQC(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method."""

    def __init__(self, disallowed_reasons: list[StudyQualityCheck]) -> None:
        self.invalid_reasons = disallowed_reasons

    def apply(self, df: DataFrame) -> DataFrame:
        """Mark the rows of the dataframe that satisfy the constraint.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        return df.filter(
            f.arrays_overlap(
                f.col("qualityControls"),
                f.array([f.lit(reason.value) for reason in self.invalid_reasons]),
            )
            == f.lit(True)
        )


class HasAllowedAnalysisFlags(MethodConstraint):
    """Class representing the constraint for the MultiSuSiE method."""

    def __init__(self, disallowed_flags: list[StudyAnalysisFlag]) -> None:
        self.disallowed_flags = disallowed_flags

    def apply(self, df: DataFrame) -> DataFrame:
        """Mark the rows of the dataframe that satisfy the constraint.

        Args:
            df (DataFrame): The input dataframe.

        Returns:
            DataFrame: The dataframe with an additional column indicating whether the constraint is satisfied.
        """
        return df.filter(
            f.arrays_overlap(
                f.col("analysisFlags"),
                f.array([f.lit(flag.value) for flag in self.disallowed_flags]),
            )
            == f.lit(True)
        )
