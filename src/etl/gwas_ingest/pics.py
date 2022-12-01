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


# TODO: Rewrite as column function.
# TODO: Drop r2 calculation
# TODO: Return null if r2 below threshold
def _aggregate_weighted_populations(associations_ancestry_ld: DataFrame) -> DataFrame:
    """Aggregation of weighted R information using ancestry proportions.

    Args:
        associations_ancestry_ld (DataFrame): Associations with ancestry and LD information

    Returns:
        DataFrame: Aggregated dataframe with weighted R information per association
    """
    #
    return (
        associations_ancestry_ld
        # To prevent error: This is reverted later by rounding to 6 dp
        .withColumn("r", f.when(f.col("r") >= 1, 0.9999995).otherwise(f.col("r")))
        # Fisher transform correlations to z-scores
        .withColumn("zscore_weighted", f.atan(f.col("r")) * f.col("relativeSampleSize"))
        # Compute weighted average across populations
        .groupBy("chromosome", "studyId", "variantId", "tagVariantId")
        .agg(
            f.sum(f.col("zscore_weighted")).alias("zscore_overall"),
            f.first("pValueMantissa").alias("pValueMantissa"),
            f.first("pValueExponent").alias("pValueExponent"),
        )
        # Inverse Fisher transform weigthed z-score back to correlation
        .withColumn("R_overall", f.round(f.tan(f.col("zscore_overall")), 6))
        .withColumn("R2_overall", f.pow(f.col("R_overall"), 2))
    )


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


def _pics_standard_deviation(neglog_p: Column, r2: Column, k: float) -> Column:
    """Compute the PICS standard deviation.

    Args:
        neglog_p (Column): Negative log p-value
        r2 (Column): R-squared
        k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

    Returns:
        Column: PICS standard deviation
    """
    return f.sqrt(1 - f.sqrt(r2) ** k) * f.sqrt(neglog_p) / 2


def _pics_mu(neglog_p: Column, r2: Column) -> Column:
    """Compute the PICS mu.

    Args:
        neglog_p (Column): Negative log p-value
        r2 (Column): R-squared

    Returns:
        Column: PICS mu
    """
    return neglog_p * r2


def _pics(associations_ld_allancestries: DataFrame, k: float) -> DataFrame:
    """Probabilistic Identification of Causal SNPs (PICS) from Farh (2014).

    Adjusts the p-values for tag SNPs based on the p-value of the lead SNP and it's LD.
    https://www.nature.com/articles/nature13835


    Args:
        associations_ld_allancestries (DataFrame): Association data with LD information. Information by ancestry needs to be aggregated first.
        k (float): Empiric constant that can be adjusted to fit the curve, 6.4 recommended.

    Returns:
        DataFrame: PICS statistics and credible sets
    """
    pics = (
        associations_ld_allancestries
        # Calculate PICS statistics
        .withColumn(
            "neglog_p",
            -1 * (f.log10(f.col("pValueMantissa")) + f.col("pValueExponent")),
        )
        .withColumn("pics_mu", _pics_mu(f.col("neglog_p"), f.col("R2_overall")))
        .withColumn(
            "pics_std",
            _pics_standard_deviation(f.col("neglog_p"), f.col("R2_overall"), k),
        )
        # Calculate PICS posterior probability
        .withColumn(
            "pics_postprob",
            _pics_posterior_probability(
                f.col("pics_mu"),
                f.col("pics_std"),
                f.col("neglog_p"),
                f.col("chromosome"),
                f.col("studyId"),
                f.col("variantId"),
            ),
        )
        # Label whether they are in the 95 or 99% credible set
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
    return pics


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
    variant_population = (
        associations.join(studies, on="studyId", how="left")
        .withColumn("gnomadSamples", f.explode(f.col("gnomadSamples")))
        .filter(
            (f.size(f.col("qualityControl")) == 0) & f.col("gnomadSamples").isNotNull()
        )
        .select(
            "variantId",
            f.col("gnomadSamples.gnomadPopulation").alias("gnomadPopulation"),
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
        )
        .distinct()
    )

    # Number of distinct variants to map:
    etl.logger.info(f"Number of variant/ancestry pairs: {variant_population.count()}")
    etl.logger.info(
        f'Number of unique variants: {variant_population.select("variantId").distinct().count()}'
    )

    # LD information for all locus and ancestries
    ld_r = ld_annotation_by_locus_ancestry(
        etl, variant_population, ld_populations, min_r2
    )
    # Saving draft data:
    etl.logger.info("LD expansion is done! Saving intermedier data.")
    ld_r.write.mode("overwrite").parquet(
        "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/ld2_table"
    )
    # Association + ancestry + ld information
    association_ancestry_ld = associations.join(
        ld_r, on=["chromosome", "variantId", "gnomadPopulation"], how="inner"
    )

    print("association_ancestry_ld:")
    print(association_ancestry_ld.columns)
    association_ancestry_ld.show(1, False, True)

    # Aggregation of weighted R information using ancestry proportions
    associations_ld_allancestries = _aggregate_weighted_populations(
        association_ancestry_ld
    )

    pics_results = _pics(associations_ld_allancestries, k)

    return pics_results.select(
        "chromosome",
        "studyId",
        "variantId",
        "tagVariantId",
        "R_overall",
        "pics_mu",
        "pics_postprob",
        "pics_95_perc_credset",
        "pics_99_perc_credset",
    )
