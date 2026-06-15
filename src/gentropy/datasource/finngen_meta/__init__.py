"""Shared models and Spark expressions for FinnGen meta-analysis ingestion."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import mac, maf
from gentropy.common.spark import reduce_add
from gentropy.config import ThreeWayMetaSumstatHarmonisationConfig as _Defaults
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE

if TYPE_CHECKING:
    from gentropy.common.session import Session

#: Maximum number of threads to use when converting BGZIP files to Parquet.
N_THREAD_MAX = 32
#: Recommended number of threads to use when converting BGZIP files to Parquet.
N_THREAD_OPTIMAL = 10


class MetaAnalysisHarmonisationConfig(BaseModel):
    """Validated configuration shared by FinnGen meta-analysis harmonisers.

    !!! note "Variant flipping logic"
    The flipping window size has to be the same as the one used for
    creating the variant direction dataset, otherwise the join will produce incorrect results.
    The default value is sourced from `gentropy.dataset.variant_direction.DEFAULT_WINDOW_SIZE`
    to keep both sides in sync.

    """

    model_config = ConfigDict(extra="forbid")

    # NOTE: Defaults below are sourced from the Hydra step config
    # `ThreeWayMetaSumstatHarmonisationConfig` (imported as `_Defaults`) so the
    # values are declared in exactly one place (`gentropy.config`). The three-way
    # config is used because it is the superset shared by both harmonisers.

    perform_meta_analysis_filter: bool = _Defaults.perform_meta_analysis_filter
    """Whether to remove variants that were not meta-analysed."""

    perform_imputation_score_filter: bool = _Defaults.perform_imputation_score_filter
    """Whether to remove variants with low imputation score (INFO)."""
    imputation_score_threshold: float = Field(
        default=_Defaults.imputation_score_threshold, ge=0.0, le=1.0
    )
    """Minimum INFO/imputation score to retain a variant. Must be in [0, 1]."""

    perform_min_allele_count_filter: bool = _Defaults.perform_min_allele_count_filter
    """Whether to remove variants with low MAC (minor allele count)."""
    min_allele_count_threshold: int = Field(
        default=_Defaults.min_allele_count_threshold, ge=1
    )
    """Minimum allele count (AC) to retain a variant. Must be >= 1."""

    perform_min_allele_frequency_filter: bool = (
        _Defaults.perform_min_allele_frequency_filter
    )
    """Whether to remove variants with low MAF (minor allele frequency)."""
    min_allele_frequency_threshold: float = Field(
        default=_Defaults.min_allele_frequency_threshold, gt=0.0, lt=0.5
    )
    """Minimum allele frequency (AF) to retain a variant. Must be in (0, 0.5)."""

    perform_samples_size_filter: bool = _Defaults.perform_samples_size_filter
    """Whether to remove variants with low sample size."""
    sample_size_threshold: int = Field(default=_Defaults.sample_size_threshold, ge=1)
    """Minimum sample size to retain a variant. Must be >= 1."""

    flipping_window_size: int = DEFAULT_WINDOW_SIZE
    """Window size (bp) used to partition the VariantDirection dataset (exact match only!).
        Defaults to `DEFAULT_WINDOW_SIZE` from `gentropy.dataset.variant_direction`.
    """
    remove_monomorphic_alleles: bool = _Defaults.remove_monomorphic_alleles
    """Whether to remove variants with equal effect and other alleles during harmonisation."""
    remove_ambiguous_alleles: bool = _Defaults.remove_ambiguous_alleles
    """Whether to remove strand-ambiguous variants (A/T or C/G).
        This filter removes only strand-ambiguous variants from reference panel,
        meaning, if the summary statistics contain the strand-ambiguous variant, not found
        in reference, it is retained without flipping.
    """
    verify_atgc: bool = _Defaults.verify_atgc
    """Whether to verify that reference and alternate alleles are valid (A, T, G, C).
        Strict ATGC validation also removes `*` (star) and `!` (multiallelic) alleles,
        so no dedicated symbol filters are required.
    """


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
                    "1,550,147 (MVP: nEUR=449,042, nAFR=121,177, nAMR=59,048; FinnGenR12: nNFE=500,349; pan-UKBB-EUR: nEUR=420,531)"
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
                # NOTE: ancestry labels must be the human-readable keys of
                # `gwas_population_2_LD_panel_map.json` (e.g. "Finnish", "European"),
                # not gnomAD codes, otherwise `aggregate_and_map_ancestries` maps
                # them to null LD populations. Keep in sync with the THREE_WAY branch.
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
                                f.col("ukbb_n_cases"), f.col("ukbb_n_controls")
                            ).alias("sampleSize"),
                            f.lit("European").alias("ancestry"),
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
                mix = f"{release.release_name}_UKB_MVP_META"
            case MetaAnalysisType.TWO_WAY:
                mix = f"{release.release_name}_UKB_META"
            case _:
                raise NotImplementedError(f"Unsupported meta-analysis type: {self}")
        return f.concat_ws("_", f.lit(mix), f.col("fg_phenotype")).alias("studyId")

    def project_id(self, release: FinnGenMetaRelease) -> Column:
        """Get the project ID column based on the meta-analysis type and release."""
        match self:
            case MetaAnalysisType.THREE_WAY:
                return f.lit(f"{release.release_name}_UKB_MVP_META").alias("projectId")
            case MetaAnalysisType.TWO_WAY:
                return f.lit(f"{release.release_name}_UKB_META").alias("projectId")
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


def convert_bgzip_to_parquet(
    session: Session,
    summary_statistics_list: list[str],
    raw_summary_statistics_output_path: str,
    finngen_release: FinnGenMetaRelease,
    meta_analysis_type: MetaAnalysisType,
    summary_statistics_schema: t.StructType,
    extract_phenotype: Callable[[Column], Column],
    n_threads: int = N_THREAD_OPTIMAL,
) -> None:
    """Convert gzipped meta-analysis summary statistics to Parquet partitioned by ``studyId``.

    This is shared by the two-way and three-way harmonisers so the ``studyId``
    is stamped as a Parquet partition column at conversion time. Downstream
    harmonisation reads ``studyId`` directly from that column rather than
    re-deriving it from the (now lost) original file name.

    !!! note "Block gzipped input files"
        Since the individual summary statistics files are **block gzipped** the
        session must have `use_enhanced_bgzip_codec` enabled for efficient reading.

    !!! note "Reading multiple files with divergent schemas"
        Individual files do not share an identical column set. ``enforceSchema``
        aligns columns positionally (breaking order) and ``inferSchema`` over a
        bulk read drops columns due to sampling. We therefore loop over the files
        with ``inferSchema`` and manually add missing columns as typed nulls,
        parallelising the loop with a thread pool.

    Args:
        session (Session): Session object.
        summary_statistics_list (list[str]): List of paths to gzipped summary statistics files.
        raw_summary_statistics_output_path (str): Output path for the Parquet files.
        finngen_release (FinnGenMetaRelease): FinnGen release identifier (e.g. ``"R12"``).
        meta_analysis_type (MetaAnalysisType): Type of meta-analysis used to build the ``studyId``.
        summary_statistics_schema (t.StructType): Superset schema the input files are aligned to.
        extract_phenotype (Callable[[Column], Column]): Function extracting ``fg_phenotype`` from a file-path column.
        n_threads (int): Maximum number of threads for the ThreadPoolExecutor (default 10).

    Raises:
        KeyError: If `use_enhanced_bgzip_codec` is set to False in the Session configuration.
    """
    if len(summary_statistics_list) == 0:
        session.logger.warning("No summary statistics paths found to process.")
        return
    if not session.use_enhanced_bgzip_codec:
        session.logger.error(
            "The use_enhanced_bgzip_codec is set to False. This will lead to inefficient reading of block gzipped files."
        )
        raise KeyError(
            "Please set `use_enhanced_bgzip_codec` to True in the Session configuration."
        )

    # Handle n_threads limits and warnings
    if not isinstance(n_threads, int) or n_threads < 1:
        session.logger.warning(
            f"Invalid n_threads value: {n_threads}. Falling back to {N_THREAD_OPTIMAL} threads."
        )
        n_threads = N_THREAD_OPTIMAL
    if n_threads < N_THREAD_OPTIMAL:
        session.logger.warning(
            f"Using low n_threads value: {n_threads}. This may lead to sub-optimal performance."
        )
    if n_threads > N_THREAD_MAX:
        session.logger.warning(
            f"Using high n_threads value: {n_threads}, this may lead to overloading spark driver. Limiting to {N_THREAD_MAX}."
        )
        n_threads = N_THREAD_MAX

    def process_one(input_path: str, session: Session, output_path: str) -> DataFrame:
        """Process one summary statistics file to the schema superset and write it out.

        Args:
            input_path (str): Input path to the gzipped summary statistics file.
            session (Session): Session object.
            output_path (str): Output path for the Parquet files.

        Returns:
            DataFrame: Processed dataframe.
        """
        df = session.spark.read.csv(
            input_path,
            header=True,
            inferSchema=True,
            sep="\t",
            enforceSchema=False,
        )
        existing_cols = set(df.columns)
        df = df.select(
            *[
                (
                    f.when(f.col(field.name) == "NA", f.lit(None))
                    .otherwise(f.col(field.name))
                    .cast(field.dataType)
                    .alias(field.name)
                    if field.name in existing_cols
                    else f.lit(None).cast(field.dataType).alias(field.name)
                )
                for field in summary_statistics_schema.fields
            ]
        )
        # Add studyId based on the input path
        df = (
            df.withColumn("fg_phenotype", extract_phenotype(f.input_file_name()))
            .withColumn("studyId", meta_analysis_type.study_id(release=finngen_release))
            .drop("fg_phenotype")
            # One file per study: the harmonisation step re-shuffles on
            # (chromosome, rangeId, variantId) anyway, so intra-study ordering
            # here buys nothing and repartitionByRange(60) is redundant overhead.
            .repartition(1)
        )
        # NOTE: Write is done per studyId partition from the thread pool to
        # make sure we do not need to collect all data after the thread execution.
        df.write.mode("append").partitionBy("studyId").parquet(output_path)
        return df

    session.logger.info(
        f"Converting gzipped summary statistics from {summary_statistics_list} to Parquet at {raw_summary_statistics_output_path}."
    )
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        list(
            pool.map(
                lambda path: process_one(
                    path,
                    session=session,
                    output_path=raw_summary_statistics_output_path,
                ),
                summary_statistics_list,
            )
        )
