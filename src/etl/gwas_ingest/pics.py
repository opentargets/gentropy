"""Calculate PICS for a given study and locus."""

from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame, Window
from scipy.stats import norm

from etl.gwas_ingest.ld import ld_annotation_by_locus_ancestry
from etl.json import data

if TYPE_CHECKING:
    from omegaconf.listconfig import ListConfig
    from pyspark.sql import Column

    from etl.common.ETLSession import ETLSession


@f.udf(t.DoubleType())
def _norm_sf(mu: float, std: float, neglog_p: float) -> float:
    """Returns the survival function of the normal distribution.

    Args:
        mu (float): mean
        std (float): standard deviation
        neglog_p (float): negative log p-value

    Returns:
        float: survival function
    """
    return float(norm(mu, std).sf(neglog_p) * 2)


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
    study_ancestry = (
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
        .join(gwascat_2_gnomad_pop, "gwas_catalog_ancestry", "left")
        # Group by sutdies and aggregate for major population:
        .groupBy("studyId", "gnomadPopulation").agg(
            f.sum(f.col("adjustedSampleSize")).alias("sampleSize")
        )
        # Calculate proportions for each study
        .withColumn(
            "relativeSampleSize",
            f.col("sampleSize") / f.sum("sampleSize").over(w_study),
        )
    )
    return study_ancestry


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
    return f.when(
        f.lag(pics_postprob, 1).over(w_credset) >= credset_probability, False
    ).otherwise(True)


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
    """
    return f.sqrt(1 - f.abs(r) ** k) * f.sqrt(neglog_p) / 2


def _pics_mu(neglog_p: Column, r: Column) -> Column:
    """Compute the PICS mu.

    Args:
        neglog_p (Column): Negative log p-value
        r (Column): R

    Returns:
        Column: PICS mu
    """
    return neglog_p * (r**2)


def _neglog_p(p_value_mantissa: Column, p_value_exponent: Column) -> Column:
    """Compute the negative log p-value.

    Args:
        p_value_mantissa (Column): P-value mantissa
        p_value_exponent (Column): P-value exponent

    Returns:
        Column: Negative log p-value
    """
    return -1 * (f.log10(p_value_mantissa) + p_value_exponent)


def pics_all_study_locus(
    etl: ETLSession,
    association_study_ancestry: DataFrame,
    ld_populations: ListConfig,
    min_r2: float,
    k: float,
) -> DataFrame:
    """Probabilistic Identification of Causal SNPs (PICS) from Farh (2014).

    Adjusts the p-values for tag SNPs based on the p-value of the lead SNP and it's LD.
    https://www.nature.com/articles/nature13835

    Args:
        etl (ETLSession): ETL session
        association_study_ancestry (DataFrame): associations with study and ancesry data
        ld_populations (ListConfig): configuration for LD populations
        min_r2 (float): Minimum R^2
        k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

    Returns:
        DataFrame: PICS statistics and credible sets
    """
    # LD information for all locus and ancestries
    ld_r = ld_annotation_by_locus_ancestry(
        etl, association_study_ancestry, ld_populations, min_r2
    )

    # Association + ancestry + ld information
    association_ancestry_ld = association_study_ancestry.join(
        ld_r, on=["chromosome", "variantId", "gnomadPopulation"], how="inner"
    )

    # Aggregate by study, lead, tag
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
        .drop("relativeSampleSize", "r").distinct()
    )

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
