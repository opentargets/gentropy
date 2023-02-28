"""Calculate PICS for a given study and locus."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame, Window
from scipy.stats import norm

from otg.common.spark_helpers import _neglog_p, adding_quality_flag
from otg.gwas_ingest.clumping import clumping
from otg.gwas_ingest.ld import ld_annotation_by_locus_ancestry
from otg.json import data

if TYPE_CHECKING:
    from omegaconf.listconfig import ListConfig
    from pyspark.sql import Column

    from otg.common.session import ETLSession


# Quality control flags:
UNRESOLVED_LEAD_FLAG = "Credible set not resolved"


@f.udf(t.DoubleType())
def _norm_sf(mu: float, std: float, neglog_p: float) -> float | None:
    """Returns the survival function of the normal distribution for the p-value.

    Args:
        mu (float): mean
        std (float): standard deviation
        neglog_p (float): negative log p-value

    Returns:
        float: survival function

    Examples:
        >>> d = [{"mu": 0, "neglog_p": 0, "std": 1}, {"mu": 1, "neglog_p": 10, "std": 10}]
        >>> spark.createDataFrame(d).withColumn("norm_sf", _norm_sf(f.col("mu"), f.col("std"), f.col("neglog_p"))).show()
        +---+--------+---+-------------------+
        | mu|neglog_p|std|            norm_sf|
        +---+--------+---+-------------------+
        |  0|       0|  1|                1.0|
        |  1|      10| 10|0.36812025069351895|
        +---+--------+---+-------------------+
        <BLANKLINE>
    """
    try:
        return float(norm(mu, std).sf(neglog_p) * 2)
    except TypeError:
        return None


def _get_study_gnomad_ancestries(etl: ETLSession, study_df: DataFrame) -> DataFrame:
    """Get all studies and their ancestries.

    Args:
        etl (ETLSession): session
        study_df (DataFrame): studies from GWAS Catalog

    Returns:
        DataFrame: studies mapped to gnomAD ancestries and their frequencies
    """
    # GWAS Catalog to gnomAD superpopulation mapping
    gwascat_2_gnomad_pop = etl.spark.createDataFrame(
        json.loads(
            pkg_resources.read_text(
                data, "gwascat_2_gnomad_superpopulation.json", encoding="utf-8"
            )
        )
    )

    # Study ancestries
    w_study = Window.partitionBy("studyId")
    return (
        study_df
        # Excluding studies where no sample discription is provided:
        .filter(f.col("discoverySamples").isNotNull())
        # Exploding sample description and study identifier:
        .withColumn("discoverySample", f.explode(f.col("discoverySamples")))
        # Splitting sample descriptions further:
        .withColumn(
            "ancestries", f.split(f.col("discoverySample.ancestry"), r",\s(?![^()]*\))")
        )
        # Dividing sample sizes assuming even distribution
        .withColumn(
            "adjustedSampleSize",
            f.col("discoverySample.sampleSize") / f.size(f.col("ancestries")),
        )
        # Exploding ancestries
        .withColumn("gwas_catalog_ancestry", f.explode(f.col("ancestries")))
        # map gwas population to gnomad superpopulation
        .join(gwascat_2_gnomad_pop, on="gwas_catalog_ancestry", how="left")
        # Group by sutdies and aggregate for major population:
        .groupBy("studyId", "gnomadPopulation")
        .agg(f.sum(f.col("adjustedSampleSize")).alias("sampleSize"))
        # Calculate proportions for each study
        .withColumn(
            "relativeSampleSize",
            f.col("sampleSize") / f.sum("sampleSize").over(w_study),
        )
        .drop("sampleSize")
    )


def _weighted_r_overall(
    chromosome: Column,
    study_id: Column,
    variant_id: Column,
    tag_variant_id: Column,
    relative_sample_size: Column,
    r: Column,
) -> Column:
    """Aggregation of weighted R information using ancestry proportions.

    Args:
        chromosome (Column): Chromosome
        study_id (Column): Study identifier
        variant_id (Column): Variant identifier
        tag_variant_id (Column): Tag variant identifier
        relative_sample_size (Column): Relative sample size
        r (Column): Correlation

    Returns:
        Column: Estimates weighted R information
    """
    pseudo_r = f.when(r >= 1, 0.9999995).otherwise(r)
    zscore_overall = f.sum(f.atan(pseudo_r) * relative_sample_size).over(
        Window.partitionBy(chromosome, study_id, variant_id, tag_variant_id)
    )
    return f.round(f.tan(zscore_overall), 6)


def _is_in_credset(
    chromosome: Column,
    study_id: Column,
    variant_id: Column,
    pics_postprob: Column,
    credset_probability: float,
) -> Column:
    """Check whether a variant is in the XX% credible set.

    Args:
        chromosome (Column): Chromosome column
        study_id (Column): Study ID column
        variant_id (Column): Variant ID column
        pics_postprob (Column): PICS posterior probability column
        credset_probability (float): Credible set probability

    Returns:
        Column: Whether the variant is in the credible set
    """
    w_cumlead = (
        Window.partitionBy(chromosome, study_id, variant_id)
        .orderBy(f.desc(pics_postprob))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    pics_postprob_cumsum = f.sum(pics_postprob).over(w_cumlead)
    w_credset = Window.partitionBy(chromosome, study_id, variant_id).orderBy(
        pics_postprob_cumsum
    )
    return (
        # If posterior probability is null, credible set flag is False:
        f.when(pics_postprob.isNull(), False)
        # If the posterior probability meets the criteria the flag is True:
        .when(
            f.lag(pics_postprob_cumsum, 1).over(w_credset) >= credset_probability, False
        )
        # IF criteria is not met, flag is False:
        .otherwise(True)
    )


def _pics_posterior_probability(
    pics_mu: Column,
    pics_std: Column,
    neglog_p: Column,
    chromosome: Column,
    study_id: Column,
    variant_id: Column,
) -> Column:
    """Compute the PICS posterior probability.

    Args:
        pics_mu (Column): PICS mu
        pics_std (Column): PICS standard deviation
        neglog_p (Column): Negative log p-value
        chromosome (Column): Chromosome column
        study_id (Column): Study ID column
        variant_id (Column): Variant ID column

    Returns:
        Column: PICS posterior probability
    """
    pics_relative_prob = f.when(pics_std == 0, 1.0).otherwise(
        _norm_sf(pics_mu, pics_std, neglog_p)
    )
    w_lead = Window.partitionBy(chromosome, study_id, variant_id)
    pics_relative_prob_sum = f.sum(pics_relative_prob).over(w_lead)
    return pics_relative_prob / pics_relative_prob_sum


def _pics_standard_deviation(neglog_p: Column, r: Column, k: float) -> Column:
    """Compute the PICS standard deviation.

    Args:
        neglog_p (Column): Negative log p-value
        r (Column): R-squared
        k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

    Returns:
        Column: PICS standard deviation

    Examples:
        >>> k = 6.4
        >>> d = [(1.0, 1.0), (10.0, 1.0), (10.0, 0.5), (100.0, 0.5), (1.0, 0.0)]
        >>> spark.createDataFrame(d).toDF("neglog_p", "r").withColumn("std", _pics_standard_deviation(f.col("neglog_p"), f.col("r"), k)).show()
        +--------+---+-----------------+
        |neglog_p|  r|              std|
        +--------+---+-----------------+
        |     1.0|1.0|              0.0|
        |    10.0|1.0|              0.0|
        |    10.0|0.5|1.571749395040553|
        |   100.0|0.5|4.970307999319905|
        |     1.0|0.0|              0.5|
        +--------+---+-----------------+
        <BLANKLINE>
    """
    return f.sqrt(1 - f.abs(r) ** k) * f.sqrt(neglog_p) / 2


def _pics_mu(neglog_p: Column, r: Column) -> Column:
    """Compute the PICS mu.

    Args:
        neglog_p (Column): Negative log p-value
        r (Column): R

    Returns:
        Column: PICS mu

    Examples:
        >>> d = [(1.0, 1.0), (10.0, 1.0), (10.0, 0.5), (100.0, 0.5), (1.0, 0.0)]
        >>> spark.createDataFrame(d).toDF("neglog_p", "r").withColumn("mu", _pics_mu(f.col("neglog_p"), f.col("r"))).show()
        +--------+---+----+
        |neglog_p|  r|  mu|
        +--------+---+----+
        |     1.0|1.0| 1.0|
        |    10.0|1.0|10.0|
        |    10.0|0.5| 2.5|
        |   100.0|0.5|25.0|
        |     1.0|0.0| 0.0|
        +--------+---+----+
        <BLANKLINE>
    """
    return neglog_p * (r**2)


def _flag_partial_mapped(
    study_id: Column, variant_id: Column, tag_variant_id: Column
) -> Column:
    """Generate flag for lead/tag pairs.

    Some lead variants can be resolved in one population but not in other. Those rows interfere with PICS calculation, so they needs to be dropped.

    Args:
        study_id (Column): Study identifier column
        variant_id (Column): Identifier of the lead variant
        tag_variant_id (Column): Identifier of the tag variant

    Returns:
        Column: Boolean

    Examples:
        >>> data = [
        ...     ('study_1', 'lead_1', 'tag_1'),  # <- keep row as tag available.
        ...     ('study_1', 'lead_1', 'tag_2'),  # <- keep row as tag available.
        ...     ('study_1', 'lead_2', 'tag_3'),  # <- keep row as tag available
        ...     ('study_1', 'lead_2', None),  # <- drop row as lead 2 is resolved.
        ...     ('study_1', 'lead_3', None)   # <- keep row as lead 3 is not resolved.
        ... ]
        >>> (
        ...     spark.createDataFrame(data, ['studyId', 'variantId', 'tagVariantId'])
        ...     .withColumn("flag_to_keep_tag", _flag_partial_mapped(f.col('studyId'), f.col('variantId'), f.col('tagVariantId')))
        ...     .show()
        ... )
        +-------+---------+------------+----------------+
        |studyId|variantId|tagVariantId|flag_to_keep_tag|
        +-------+---------+------------+----------------+
        |study_1|   lead_3|        null|            true|
        |study_1|   lead_1|       tag_1|            true|
        |study_1|   lead_1|       tag_2|            true|
        |study_1|   lead_2|       tag_3|            true|
        |study_1|   lead_2|        null|           false|
        +-------+---------+------------+----------------+
        <BLANKLINE>
    """
    has_resolved_tag = f.when(
        f.array_contains(
            f.collect_set(tag_variant_id.isNotNull()).over(
                Window.partitionBy(study_id, variant_id)
            ),
            True,
        ),
        True,
    ).otherwise(False)

    return f.when(tag_variant_id.isNotNull() | ~has_resolved_tag, True).otherwise(False)


def calculate_pics(
    associations_ld_allancestries: DataFrame,
    k: float,
) -> DataFrame:
    """It calculates the PICS statistics for each variant in the input DataFrame.

    Args:
        associations_ld_allancestries (DataFrame): DataFrame
        k (float): float

    Returns:
        A dataframe with the columns:
            - pics_mu
            - pics_std
            - pics_postprob
            - pics_95_perc_credset
            - pics_99_perc_credset
    """
    # Calculate and return PICS statistics
    return (
        associations_ld_allancestries.withColumn(
            "pics_mu",
            _pics_mu(
                _neglog_p(f.col("pValueMantissa"), f.col("pValueExponent")),
                f.col("R_overall"),
            ),
        )
        .withColumn(
            "pics_std",
            _pics_standard_deviation(
                _neglog_p(f.col("pValueMantissa"), f.col("pValueExponent")),
                f.col("R_overall"),
                k,
            ),
        )
        # Some overall_R are suspiciously high. For such cases, pics_std fails. Drop them:
        .filter(~f.isnan(f.col("pics_std")))
        .withColumn(
            "pics_postprob",
            _pics_posterior_probability(
                f.col("pics_mu"),
                f.col("pics_std"),
                _neglog_p(f.col("pValueMantissa"), f.col("pValueExponent")),
                f.col("chromosome"),
                f.col("studyId"),
                f.col("variantId"),
            ),
        )
        .withColumn(
            "pics_95_perc_credset",
            _is_in_credset(
                f.col("chromosome"),
                f.col("studyId"),
                f.col("variantId"),
                f.col("pics_postprob"),
                0.95,
            ),
        )
        .withColumn(
            "pics_99_perc_credset",
            _is_in_credset(
                f.col("chromosome"),
                f.col("studyId"),
                f.col("variantId"),
                f.col("pics_postprob"),
                0.99,
            ),
        )
    )


def pics_all_study_locus(
    etl: ETLSession,
    associations: DataFrame,
    studies: DataFrame,
    ld_populations: ListConfig,
    min_r2: float,
    k: float,
) -> DataFrame:
    """Calculates study-locus based on PICS.

    It takes in a dataframe of associations, a dataframe of studies, a list of LD populations, a minimum
    R^2, and a constant k, and returns a dataframe of PICS results

    Args:
        etl (ETLSession): ETLSession
        associations (DataFrame): DataFrame
        studies (DataFrame): DataFrame
        ld_populations (ListConfig): ListConfig = ListConfig(
        min_r2 (float): Minimum R^2
        k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

    Returns:
        DataFrame: _description_
    """
    # Extracting ancestry information from study table, then map to gnomad population:
    gnomad_mapped_studies = _get_study_gnomad_ancestries(
        etl, studies.withColumnRenamed("id", "studyId")
    )

    # Joining to associations:
    association_gnomad = associations.join(
        gnomad_mapped_studies,
        on="studyId",
        how="left",
    ).persist()

    # Extracting mapped variants for LD expansion:
    variant_population = (
        association_gnomad.filter(f.col("position").isNotNull())
        .select(
            "variantId",
            "gnomadPopulation",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
        )
        .distinct()
    )

    # LD information for all locus and ancestries
    ld_r = ld_annotation_by_locus_ancestry(
        etl, variant_population, ld_populations, min_r2
    ).persist()

    # Joining association with linked variants (while keeping unresolved associations).
    association_ancestry_ld = (
        association_gnomad.join(
            ld_r, on=["chromosome", "variantId", "gnomadPopulation"], how="left"
        )
        # It is possible that a lead variant is resolved in the LD panel of a population, but not in the other.
        # Once the linked variants are joined this leads to unnecessary nulls. To avoid this, adding
        # a flag indicating if a lead variant is resolved in the LD panel for at least one population:
        .withColumn(
            "flag_to_keep_tag",
            _flag_partial_mapped(
                f.col("studyId"), f.col("variantId"), f.col("tagVariantId")
            ),
        )
        # Dropping rows where no tag is available, however the lead is resolved:
        .filter(f.col("flag_to_keep_tag"))
        # Dropping unused column:
        .drop("flag_to_keep_tag")
        # Updating qualityControl for unresolved associations:
        .withColumn(
            "qualityControl",
            adding_quality_flag(
                f.col("qualityControl"),
                f.col("tagVariantId").isNull(),
                UNRESOLVED_LEAD_FLAG,
            ),
        ).distinct()
    )

    # Aggregation of weighted R information using ancestry proportions
    associations_ld_allancestries = (
        association_ancestry_ld.withColumn(
            "R_overall",
            _weighted_r_overall(
                f.col("chromosome"),
                f.col("studyId"),
                f.col("variantId"),
                f.col("tagVariantId"),
                f.col("relativeSampleSize"),
                f.col("r"),
            ),
        )
        # Collapse the data by study, lead, tag
        .drop("relativeSampleSize", "r", "gnomadPopulation").distinct()
        # Clumping non-independent associations together:
        .transform(clumping)
    )

    return calculate_pics(associations_ld_allancestries, k)
