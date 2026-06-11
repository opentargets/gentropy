"""Shared models and Spark expressions for FinnGen meta-analysis ingestion."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import mac, maf
from gentropy.common.spark import reduce_add
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE


class MetaAnalysisHarmonisationConfig(BaseModel):
    """Validated configuration shared by FinnGen meta-analysis harmonisers.

    !!! note "Variant flipping logic"
    The flipping window size has to be the same as the one used for
    creating the variant direction dataset, otherwise the join will produce incorrect results.
    The default value is sourced from `gentropy.dataset.variant_direction.DEFAULT_WINDOW_SIZE`
    to keep both sides in sync.

    """

    model_config = ConfigDict(extra="forbid")

    perform_meta_analysis_filter: bool = True
    """Whether to remove variants that were not meta-analysed."""

    perform_imputation_score_filter: bool = True
    """Whether to remove variants with low imputation score (INFO)."""
    imputation_score_threshold: float = Field(default=0.8, ge=0.0, le=1.0)
    """Minimum INFO/imputation score to retain a variant. Must be in [0, 1]."""

    perform_min_allele_count_filter: bool = True
    """Whether to remove variants with low MAC (minor allele count)."""
    min_allele_count_threshold: int = Field(default=20, ge=1)
    """Minimum allele count (AC) to retain a variant. Must be >= 1."""

    perform_min_allele_frequency_filter: bool = False
    """Whether to remove variants with low MAF (minor allele frequency)."""
    min_allele_frequency_threshold: float = Field(default=1e-4, gt=0.0, lt=0.5)
    """Minimum allele frequency (AF) to retain a variant. Must be in (0, 0.5)."""

    perform_samples_size_filter: bool = True
    """Whether to remove variants with low sample size."""
    sample_size_threshold: int = Field(default=1000, ge=1)
    """Minimum sample size to retain a variant. Must be >= 1."""

    flipping_window_size: int = DEFAULT_WINDOW_SIZE
    """Window size (bp) used to partition the VariantDirection dataset (exact match only!).
        Defaults to `DEFAULT_WINDOW_SIZE` from `gentropy.dataset.variant_direction`.
    """
    remove_star_alleles: bool = True
    """Whether to remove variants with `*` alleles during harmonisation."""
    remove_monomorphic_alleles: bool = True
    """Whether to remove variants with equal effect and other alleles during harmonisation."""
    remove_ambiguous_alleles: bool = False
    """Whether to remove strand-ambiguous variants (A/T or C/G).
        This filter removes only strand-ambiguous variants from reference panel,
        meaning, if the summary statistics contain the strand-ambiguous variant, not found
        in reference, it is retained without flipping.
    """
    remove_multiallelic_alleles: bool = True
    """Whether to remove variants with multiple other alleles during harmonisation.
        These alleles are marked as ! in summary statistics. For the sake of flipping, we
        remove these from both effect and other allele columns.
    """
    verify_atgc: bool = True
    """Whether to verify that reference and alternate alleles are valid (A, T, G, C)."""

    @model_validator(mode="after")
    def validate_filters(self) -> Self:
        """Disable redundant symbol filters when strict ATGC validation is enabled."""
        if self.verify_atgc:
            # Strict ATGC validation also removes '*' and '!' alleles.
            self.remove_star_alleles = False
            self.remove_multiallelic_alleles = False

        return self


THREE_WAY_MANIFEST_SCHEMA = t.StructType(
    [
        t.StructField("fg_phenotype", t.StringType(), nullable=True),
        t.StructField("name", t.StringType(), nullable=True),
        t.StructField("category", t.StringType(), nullable=True),
        t.StructField("category_index", t.IntegerType(), nullable=True),
        t.StructField("fg_n_cases", t.IntegerType(), nullable=True),
        t.StructField("fg_n_controls", t.IntegerType(), nullable=True),
        t.StructField("ukbb_phenotype", t.StringType(), nullable=True),
        t.StructField("ukbb_n_cases", t.IntegerType(), nullable=True),
        t.StructField("ukbb_n_controls", t.IntegerType(), nullable=True),
        t.StructField("ukbb_phecode_type", t.StringType(), nullable=True),
        t.StructField("ukbb_phecode", t.StringType(), nullable=True),
        t.StructField("ukbb_phecode_sex", t.StringType(), nullable=True),
        t.StructField("mvp_phenotype", t.StringType(), nullable=True),
        t.StructField("MVP_AFR_n_cases", t.IntegerType(), nullable=True),
        t.StructField("MVP_EUR_n_cases", t.IntegerType(), nullable=True),
        t.StructField("MVP_AMR_n_cases", t.IntegerType(), nullable=True),
        t.StructField("MVP_AFR_n_controls", t.IntegerType(), nullable=True),
        t.StructField("MVP_EUR_n_controls", t.IntegerType(), nullable=True),
        t.StructField("MVP_AMR_n_controls", t.IntegerType(), nullable=True),
        t.StructField("path_bucket", t.StringType(), nullable=True),
        t.StructField("path_https", t.StringType(), nullable=True),
    ]
)

TWO_WAY_MANIFEST_SCHEMA = t.StructType(
    [
        t.StructField("fg_phenotype", t.StringType(), nullable=True),
        t.StructField("name", t.StringType(), nullable=True),
        t.StructField("category", t.StringType(), nullable=True),
        t.StructField("category_index", t.IntegerType(), nullable=True),
        t.StructField("fg_n_cases", t.IntegerType(), nullable=True),
        t.StructField("fg_n_controls", t.IntegerType(), nullable=True),
        t.StructField("ukbb_phenotype", t.StringType(), nullable=True),
        t.StructField("ukbb_is_custom", t.StringType(), nullable=True),
        t.StructField("ukbb_n_cases", t.IntegerType(), nullable=True),
        t.StructField("ukbb_n_controls", t.IntegerType(), nullable=True),
        t.StructField(
            "ukbb_definition_cohort_include_diagnosis", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_cohort_include_atc", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_cohort_other_criteria", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_cohort_exclude_diagnosis", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_cohort_exclude_atc", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_control_exclude_diagnosis", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_control_exclude_atc", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_control_exclude_other", t.StringType(), nullable=True
        ),
        t.StructField(
            "ukbb_definition_control_include_other", t.StringType(), nullable=True
        ),
        t.StructField("ukbb_definition_sex", t.StringType(), nullable=True),
    ]
)


class FinnGenMetaRelease(BaseModel):
    """Model representing a FinnGen release."""

    release: Annotated[str, StringConstraints(pattern="R\\d+")]
    """FinnGen release identifier (e.g. "R12")."""

    @property
    def release_name(self) -> str:
        """Get the release name.

        Returns:
            str: Namespaced release name (e.g. ``"FINNGEN_R12"``).
        """
        return f"FINNGEN_{self.release}"


class MetaAnalysisType(str, Enum):
    """Supported FinnGen meta-analysis cohort combinations."""

    THREE_WAY = "THREE_WAY"
    """Corresponds to the FinnGen x UKBB x MVP meta-analysis."""
    TWO_WAY = "TWO_WAY"
    """Corresponds to the FinnGen x UKBB meta-analysis."""

    def publication_date(self) -> Column:
        """Get the publication date column based on the meta-analysis type.

        Returns:
            Column: Spark Column representing the publication date.
        """
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.lit("2025-12-01")
            case MetaAnalysisType.TWO_WAY:
                return f.lit("2024-11-01")
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def initial_sample_size(self) -> Column:
        """Get the initial sample size column based on the meta-analysis type.

        Returns:
            Column: Spark Column representing the initial sample size.
        """
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.lit(
                    "1,550,147 (MVP: nEUR=449,042, nAFR=121,177, nAMR=59,048; FinnGenR13: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
                )  # based on https://metaresults-ukbb.finngen.fi/about
            case MetaAnalysisType.TWO_WAY:
                return f.lit(
                    "920,880 (FinnGenR12: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
                )  # based on https://metaresults-ukbb.finngen.fi/about
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def n_samples_per_cohort(self) -> Column:
        """Get the number of samples per cohort column.

        Returns:
            Column: Spark Column representing the number of samples per cohort.
        """
        n_samples = [
            f.struct(
                f.lit("FinnGen").alias("cohort"),
                reduce_add(f.col("fg_n_cases"), f.col("fg_n_controls")).alias(
                    "nSamples"
                ),
            ),
            f.struct(
                f.lit("UKBB").alias("cohort"),
                reduce_add(f.col("ukbb_n_cases"), f.col("ukbb_n_controls")).alias(
                    "nSamples"
                ),
            ),
        ]
        match self:
            case MetaAnalysisType.TWO_WAY:
                return f.array(*n_samples).alias("nSamplesPerCohort")
            case MetaAnalysisType.THREE_WAY:
                n_samples += [
                    f.struct(
                        f.lit("MVP_EUR").alias("cohort"),
                        reduce_add(
                            f.col("MVP_EUR_n_cases"), f.col("MVP_EUR_n_controls")
                        ).alias("nSamples"),
                    ),
                    f.struct(
                        f.lit("MVP_AFR").alias("cohort"),
                        reduce_add(
                            f.col("MVP_AFR_n_cases"), f.col("MVP_AFR_n_controls")
                        ).alias("nSamples"),
                    ),
                    f.struct(
                        f.lit("MVP_AMR").alias("cohort"),
                        reduce_add(
                            f.col("MVP_AMR_n_cases"), f.col("MVP_AMR_n_controls")
                        ).alias("nSamples"),
                    ),
                ]

                return f.array(*n_samples).alias("nSamplesPerCohort")
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def n_cases_per_cohort(self) -> Column:
        """Get the number of cases per cohort column.

        Returns:
            Column: Spark Column representing the number of cases per cohort.
        """
        n_cases = [
            f.struct(
                f.lit("FinnGen").alias("cohort"),
                f.coalesce(f.col("fg_n_cases"), f.lit(0)).alias("nCases"),
            ),
            f.struct(
                f.lit("UKBB").alias("cohort"),
                f.coalesce(f.col("ukbb_n_cases"), f.lit(0)).alias("nCases"),
            ),
        ]
        match self:
            case MetaAnalysisType.TWO_WAY:
                return f.array(*n_cases).alias("nCasesPerCohort")
            case MetaAnalysisType.THREE_WAY:
                n_cases += [
                    f.struct(
                        f.lit("MVP_EUR").alias("cohort"),
                        f.coalesce(f.col("MVP_EUR_n_cases"), f.lit(0)).alias("nCases"),
                    ),
                    f.struct(
                        f.lit("MVP_AFR").alias("cohort"),
                        f.coalesce(f.col("MVP_AFR_n_cases"), f.lit(0)).alias("nCases"),
                    ),
                    f.struct(
                        f.lit("MVP_AMR").alias("cohort"),
                        f.coalesce(f.col("MVP_AMR_n_cases"), f.lit(0)).alias("nCases"),
                    ),
                ]
                return f.array(*n_cases).alias("nCasesPerCohort")
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def discovery_samples(self) -> Column:
        """Get the discovery samples column based on the meta-analysis type."""
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.filter(
                    f.array(
                        f.struct(
                            reduce_add(
                                f.col("fg_n_cases"), f.col("fg_n_controls")
                            ).alias("sampleSize"),
                            f.lit("Finnish").alias("ancestry"),
                        ),
                        f.struct(
                            reduce_add(
                                f.col("ukbb_n_cases"),
                                f.col("ukbb_n_controls"),
                                f.col("MVP_EUR_n_cases"),
                                f.col("MVP_EUR_n_controls"),
                            ).alias("sampleSize"),
                            f.lit("European").alias("ancestry"),
                        ),
                        f.struct(
                            reduce_add(
                                f.col("MVP_AFR_n_cases"), f.col("MVP_AFR_n_controls")
                            ).alias("sampleSize"),
                            f.lit("African").alias("ancestry"),
                        ),
                        f.struct(
                            reduce_add(
                                f.col("MVP_AMR_n_cases"), f.col("MVP_AMR_n_controls")
                            ).alias("sampleSize"),
                            f.lit("Admixed American").alias("ancestry"),
                        ),
                    ),
                    lambda x: x.sampleSize > 0.0,
                ).alias("discoverySamples")
            case MetaAnalysisType.TWO_WAY:
                return f.filter(
                    f.array(
                        f.struct(
                            reduce_add(
                                f.col("fg_n_cases"), f.col("fg_n_controls")
                            ).alias("sampleSize"),
                            f.lit("fin").alias("ancestry"),
                        ),
                        f.struct(
                            reduce_add(
                                f.col("ukbb_n_cases"), f.col("ukbb_n_controls")
                            ).alias("sampleSize"),
                            f.lit("nfe").alias("ancestry"),
                        ),
                    ),
                    lambda x: x.sampleSize > 0.0,
                ).alias("discoverySamples")
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def n_cases(self) -> Column:
        """Get the total number of cases column based on the meta-analysis type."""
        ancestry_cols = [c for c in self._get_required_columns() if "n_cases" in c]
        return reduce_add(*[f.col(c) for c in ancestry_cols]).alias("nCases")

    def n_samples(self) -> Column:
        """Get the total number of samples column based on the meta-analysis type."""
        ancestry_cols = [
            c
            for c in self._get_required_columns()
            if "n_cases" in c or "n_controls" in c
        ]
        return reduce_add(*[f.col(c) for c in ancestry_cols]).alias("nSamples")

    def n_controls(self) -> Column:
        """Get the total number of controls column based on the meta-analysis type."""
        ancestry_cols = [c for c in self._get_required_columns() if "n_controls" in c]
        return reduce_add(*[f.col(c) for c in ancestry_cols]).alias("nControls")

    def cohorts(self) -> Column:
        """Build the Spark array of cohorts included in the meta-analysis.

        Returns:
            Column: Array column containing cohort identifiers.
        """
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.array(
                    [
                        f.lit("FINNGEN"),
                        f.lit("pan-UKBB-EUR"),
                        f.lit("MVP_EUR"),
                        f.lit("MVP_AFR"),
                        f.lit("MVP_AMR"),
                    ]
                )
            case MetaAnalysisType.TWO_WAY:
                return f.array([f.lit("FINNGEN"), f.lit("pan-UKBB-EUR")])
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def study_id(self, release: FinnGenMetaRelease) -> Column:
        """Get the study ID column based on the meta-analysis type and release."""
        match self:
            case MetaAnalysisType.THREE_WAY:
                mix = f"{release.release_name}_UKBB_MVP_META"
            case MetaAnalysisType.TWO_WAY:
                mix = f"{release.release_name}_UKBB_META"
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")
        return f.concat_ws("_", f.lit(mix), f.col("fg_phenotype")).alias("studyId")

    def project_id(self, release: FinnGenMetaRelease) -> Column:
        """Get the project ID column based on the meta-analysis type and release."""
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.lit(f"{release.release_name}_UKBB_MVP_META").alias("projectId")
            case MetaAnalysisType.TWO_WAY:
                return f.lit(f"{release.release_name}_UKBB_META").alias("projectId")
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def _get_required_columns(self) -> set[str]:
        """Get the set of required columns for the meta-analysis manifest.

        Returns:
            set[str]: Set of required column names.
        """
        match self:
            case MetaAnalysisType.THREE_WAY:
                return {
                    "fg_phenotype",
                    "name",
                    "fg_n_cases",
                    "fg_n_controls",
                    "ukbb_n_cases",
                    "ukbb_n_controls",
                    "MVP_EUR_n_cases",
                    "MVP_EUR_n_controls",
                    "MVP_AFR_n_cases",
                    "MVP_AFR_n_controls",
                    "MVP_AMR_n_cases",
                    "MVP_AMR_n_controls",
                }
            case MetaAnalysisType.TWO_WAY:
                return {
                    "fg_phenotype",
                    "name",
                    "fg_n_cases",
                    "fg_n_controls",
                    "ukbb_n_cases",
                    "ukbb_n_controls",
                }
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")

    def get_manifest_schema(self) -> t.StructType:
        """Get the Spark schema for the meta-analysis manifest.

        Returns:
            t.StructType: Spark schema for the meta-analysis manifest.

        """
        match self:
            case MetaAnalysisType.TWO_WAY:
                return TWO_WAY_MANIFEST_SCHEMA
            case MetaAnalysisType.THREE_WAY:
                return THREE_WAY_MANIFEST_SCHEMA


def has_low_min_allele_count(
    min_allele_count: Column, min_allele_count_threshold: int = 20
) -> Column:
    """Find if variant has a low minor allele count in any of the cohorts.

    Note:
        If any cohort has a minor allele count below the threshold, the variant is considered to have a low minor allele count.


    Args:
        min_allele_count (Column): Column containing array of structs with `cohort` and `minAlleleCount` fields.
        min_allele_count_threshold (int): Threshold below which the minor allele count is considered low.

    Returns:
        Column: Boolean column indicating if any cohort has a low minor allele count.

    Examples:
        >>> data = [("v1", [{"cohort": "A", "minAlleleCount": 30}, {"cohort": "B", "minAlleleCount": 25}]),
        ...         ("v2", [{"cohort": "A", "minAlleleCount": 15}, {"cohort": "B", "minAlleleCount": 25}]),
        ...         ("v3", [{"cohort": "A", "minAlleleCount": 30}, {"cohort": "B", "minAlleleCount": 10}]),
        ...         ("v4", [{"cohort": "A", "minAlleleCount": 5},],)]
        >>> schema = "variantId STRING, cohortMinAlleleCount ARRAY<STRUCT<cohort: STRING, minAlleleCount: INT>>"
        >>> df = spark.createDataFrame(data, schema)
        >>> df = df.withColumn("hasLowMinAlleleCount", has_low_min_allele_count(f.col("cohortMinAlleleCount"), 20))
        >>> df.select("variantId", "hasLowMinAlleleCount").show()
        +---------+--------------------+
        |variantId|hasLowMinAlleleCount|
        +---------+--------------------+
        |       v1|               false|
        |       v2|                true|
        |       v3|                true|
        |       v4|                true|
        +---------+--------------------+
        <BLANKLINE>

    """
    return (
        f.size(
            f.filter(
                min_allele_count,
                lambda x: (
                    x.getField("minAlleleCount") < f.lit(min_allele_count_threshold)
                ),
            )
        )
        > 0
    ).alias("hasLowMinAlleleCount")


def min_allele_count(
    cohort_min_allele_frequency: Column, n_samples_per_cohort: Column
) -> Column:
    """Minor Allele Count (MAC) per cohort.

    Note:
        If a cohort either do not have the maf or nCases, it will be dropped from the resulting MAC array.

    Args:
        cohort_min_allele_frequency (Column): Column containing array of structs with `cohort` and `minAlleleFrequency` fields.
        n_samples_per_cohort (Column): Column containing array of structs with `cohort` and `nSamples` fields.

    Returns:
        Column: Column containing array of structs with `cohort` and `minAlleleCount` fields.

    Examples:
        >>> maf = {"v1": [{"cohort": "A", "minAlleleFrequency": 0.1}, {"cohort": "B", "minAlleleFrequency": 0.2}],
        ...         "v2": [{"cohort": "A", "minAlleleFrequency": 0.05}, {"cohort": "D", "minAlleleFrequency": 0.15}],
        ...         "v3": [{"cohort": "A", "minAlleleFrequency": 0.01}, {"cohort": "B", "minAlleleFrequency": 0.02}],}
        >>> n_samples = {"v1": [{"cohort": "A", "nSamples": 100}, {"cohort": "B", "nSamples": 200}],
        ...            "v2": [{"cohort": "A", "nSamples": 150}, {"cohort": "C", "nSamples": 250}],
        ...            "v3": [{"cohort": "C", "nSamples": 50}, {"cohort": "D", "nSamples": 80}],}
        >>> data = [("v1", maf["v1"], n_samples["v1"]),
        ...         ("v2", maf["v2"], n_samples["v2"]),
        ...         ("v3", maf["v3"], n_samples["v3"]),]
        >>> schema = "variantId STRING, cohortMinAlleleFrequency ARRAY<STRUCT<cohort: STRING, minAlleleFrequency: DOUBLE>>, nSamplesPerCohort ARRAY<STRUCT<cohort: STRING, nSamples: INT>>"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+------------------------+--------------------+
        |variantId|cohortMinAlleleFrequency|nSamplesPerCohort   |
        +---------+------------------------+--------------------+
        |v1       |[{A, 0.1}, {B, 0.2}]    |[{A, 100}, {B, 200}]|
        |v2       |[{A, 0.05}, {D, 0.15}]  |[{A, 150}, {C, 250}]|
        |v3       |[{A, 0.01}, {B, 0.02}]  |[{C, 50}, {D, 80}]  |
        +---------+------------------------+--------------------+
        <BLANKLINE>
        >>> df = df.withColumn("cohortMinAlleleCount", min_allele_count(f.col("cohortMinAlleleFrequency"), f.col("nSamplesPerCohort")))
        >>> df.select("variantId", "cohortMinAlleleCount").show(truncate=False)
        +---------+--------------------+
        |variantId|cohortMinAlleleCount|
        +---------+--------------------+
        |v1       |[{A, 20}, {B, 80}]  |
        |v2       |[{A, 15}]           |
        |v3       |[]                  |
        +---------+--------------------+
        <BLANKLINE>

    """
    return f.transform(
        f.filter(
            cohort_min_allele_frequency,
            lambda left: f.exists(
                n_samples_per_cohort,
                lambda right: right.getField("cohort") == left.getField("cohort"),
            ),
        ),
        lambda left: f.struct(
            left.getField("cohort").alias("cohort"),
            mac(
                left.getField("minAlleleFrequency"),
                f.filter(
                    n_samples_per_cohort,
                    lambda right: right.getField("cohort") == left.getField("cohort"),
                )
                .getItem(0)
                .getField("nSamples"),
            ).alias("minAlleleCount"),
        ),
    )


def min_allele_frequency(allele_freq: Column) -> Column:
    """Minor Allele Frequency (MAF) per cohort.

    Note:
        The resulting value is of DecimalType(11, 10) to ensure precision for low frequency variants.

    Args:
        allele_freq (Column): Column containing array of structs with `cohort` and `alleleFrequency` fields.

    Returns:
        Column: Column containing array of structs with `cohort` and `minAlleleFrequency` fields.

    Examples:
        >>> data = [("v1", [{"cohort": "A", "alleleFrequency": 0.1}, {"cohort": "B", "alleleFrequency": 0.7}]),]
        >>> schema = "variantId STRING, alleleFrequencies ARRAY<STRUCT<cohort: STRING, alleleFrequency: DOUBLE>>"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+--------------------+
        |variantId|alleleFrequencies   |
        +---------+--------------------+
        |v1       |[{A, 0.1}, {B, 0.7}]|
        +---------+--------------------+
        <BLANKLINE>

        >>> df = df.withColumn("cohortMinAlleleFrequency", min_allele_frequency(f.col("alleleFrequencies")))
        >>> df.show(truncate=False)
        +---------+--------------------+--------------------------------------+
        |variantId|alleleFrequencies   |cohortMinAlleleFrequency              |
        +---------+--------------------+--------------------------------------+
        |v1       |[{A, 0.1}, {B, 0.7}]|[{A, 0.1000000000}, {B, 0.3000000000}]|
        +---------+--------------------+--------------------------------------+
        <BLANKLINE>
    """
    return f.transform(
        allele_freq,
        lambda x: f.struct(
            x.getField("cohort").alias("cohort"),
            maf(x.getField("alleleFrequency")).alias("minAlleleFrequency"),
        ),
    )


def has_low_min_allele_frequency(maf: Column, threshold: float = 1e-4) -> Column:
    """Find if variant has a low minor allele frequency in any cohort.

    Args:
        maf (Column): Column containing array of structs with `cohort` and `minAlleleFrequency` fields.
        threshold (float): Threshold below which the minor allele frequency is considered low. Default is 1e-4.

    Returns:
        Column: Boolean column indicating if any cohort has a low minor allele frequency.

    Examples:
        >>> maf = {"v1": [{"cohort": "A", "minAlleleFrequency": 0.0001}, {"cohort": "B", "minAlleleFrequency": 0.0002}],
        ...         "v2": [{"cohort": "A", "minAlleleFrequency": None}, {"cohort": "D", "minAlleleFrequency": 0.15}],
        ...         "v3": [{"cohort": "A", "minAlleleFrequency": 0.00001}, {"cohort": "B", "minAlleleFrequency": 0.2}],}
        >>> data = [("v1", maf["v1"]),
        ...         ("v2", maf["v2"]),
        ...         ("v3", maf["v3"]),]
        >>> schema = "variantId STRING, cohortMinAlleleFrequency ARRAY<STRUCT<cohort: STRING, minAlleleFrequency: DOUBLE>>"
        >>> df = spark.createDataFrame(data, schema)
        >>> df.show(truncate=False)
        +---------+--------------------------+
        |variantId|cohortMinAlleleFrequency  |
        +---------+--------------------------+
        |v1       |[{A, 1.0E-4}, {B, 2.0E-4}]|
        |v2       |[{A, NULL}, {D, 0.15}]    |
        |v3       |[{A, 1.0E-5}, {B, 0.2}]   |
        +---------+--------------------------+
        <BLANKLINE>

        >>> df = df.withColumn("hasMinAlleleFrequency", has_low_min_allele_frequency(f.col("cohortMinAlleleFrequency")))
        >>> df.select("variantId", "hasMinAlleleFrequency").show(truncate=False)
        +---------+---------------------+
        |variantId|hasMinAlleleFrequency|
        +---------+---------------------+
        |v1       |false                |
        |v2       |false                |
        |v3       |true                 |
        +---------+---------------------+
        <BLANKLINE>
    """
    non_empty_maf = f.filter(
        maf, lambda x: x.getField("minAlleleFrequency").isNotNull()
    )

    n_cohorts_with_maf_below_threshold = f.size(
        f.filter(
            non_empty_maf,
            lambda x: (
                x.getField("minAlleleFrequency")
                < f.lit(threshold).cast(t.DecimalType(11, 10))
            ),
        )
    )
    return n_cohorts_with_maf_below_threshold > 0
