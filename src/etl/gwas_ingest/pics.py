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


def pics_study_locus(
    etl: ETLSession,
    associations_df: DataFrame,
    study_df: DataFrame,
    ld_populations: ListConfig,
    min_r2: float,
) -> DataFrame:
    """Calculates study-locus based on PICS.

    Args:
        etl (ETLSession): ETL session
        associations_df (DataFrame): associations
        study_df (DataFrame): studies
        ld_populations (ListConfig): configuration for LD populations
        min_r2 (float): Minimum R^2

    Returns:
        DataFrame: _description_
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
    w_study = Window.partitionBy("studyAccession")
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
        # TODO: track unmapped populations?
        .join(gwascat_2_gnomad_pop, "gwas_catalog_ancestry", "left")
        # Group by stdies and aggregate for major population:
        .groupBy("studyAccession", "gnomad_ancestry").agg(
            f.sum(f.col("adjustedSampleSize")).alias("sampleSize")
        )
        # Calculate proportions for each study
        .withColumn(
            "ancestry_proportion",
            f.col("sampleSize") / f.sum("sampleSize").over(w_study),
        )
    )

    # Joining the associations_df and study_ancestry dataframes on the studyAccession column.
    association_ancestry = (
        associations_df.filter(f.size("flag") == 0)
        .select("variantId", "studyAccession", "pValueMantissa", "pValueExponent")
        .join(study_ancestry, on="studyAccession", how="inner")
        .select(
            "*",
            f.split(f.col("variantId"), "_").getItem(0).alias("chromosome"),
            f.split(f.col("variantId"), "_").getItem(1).cast("int").alias("position"),
            f.split(f.col("variantId"), "_").getItem(2).alias("referenceAllele"),
            f.split(f.col("variantId"), "_").getItem(3).alias("alternateAllele"),
        )
        .distinct()
        .persist()
    )

    # LD information for all locus and ancestries
    ld_r = ld_annotation_by_locus_ancestry(
        etl, association_ancestry, ld_populations, min_r2
    )

    # Empiric constant that can be adjusted to fit the curve, 6.4 recommended.
    k = 6.4

    w_lead = Window.partitionBy("chromosome", "studyAccession", "variantId")
    w_cumlead = (
        Window.partitionBy("chromosome", "studyAccession", "variantId")
        .orderBy(f.desc("pics_postprob"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_credset = Window.partitionBy("chromosome", "studyAccession", "variantId").orderBy(
        "pics_postprob_cumsum"
    )
    association_ancestry_ld = (
        association_ancestry.join(
            ld_r, on=["chromosome", "variantId", "gnomad_ancestry"], how="inner"
        )
        # To prevent error: This is reverted later by rounding to 6 dp
        .withColumn("r", f.when(f.col("r") == 1, 0.9999995).otherwise(f.col("r")))
        # Fisher transform correlations to z-scores
        .withColumn(
            "zscore_weighted", f.atan(f.col("r")) * f.col("ancestry_proportion")
        )
        # Compute weighted average across populations
        .groupBy("chromosome", "studyAccession", "variantId", "tagVariantId")
        .agg(
            f.sum(f.col("zscore_weighted")).alias("zscore_overall"),
            f.first("pValueMantissa").alias("pValueMantissa"),
            f.first("pValueExponent").alias("pValueExponent"),
        )
        # Inverse Fisher transform weigthed z-score back to correlation
        .withColumn("R_overall", f.tan(f.col("zscore_overall")))
        .withColumn("R_overall", f.round(f.col("R_overall"), 6))
        .withColumn("R2_overall", f.pow(f.col("R_overall"), 2))
        # Probabilistic Identification of Causal SNPs (PICS) from Farh (2014):
        # https://www.nature.com/articles/nature13835
        # Adjusts the p-values for tag SNPs based on the p-value of the lead SNP
        # and it's LD.
        # Calculate PICS statistics
        .withColumn(
            "neglog_p",
            -1 * (f.log10(f.col("pValueMantissa")) + f.col("pValueExponent")),
        )
        .withColumn("pics_mu", f.col("neglog_p") * f.col("R2_overall"))
        .withColumn(
            "pics_std",
            f.sqrt(1 - f.sqrt(f.col("R2_overall")) ** k)
            * f.sqrt(f.col("neglog_p"))
            / 2,
        )
        .withColumn(
            "pics_relative_prob",
            f.when(f.col("pics_std") == 0, 1.0).otherwise(
                _norm_sf(f.col("pics_mu"), f.col("pics_std"), f.col("neglog_p"))
            ),
        )
        # Calculate the sum of the posterior probabilities at each locus
        .withColumn("pics_relative_prob_sum", f.sum("pics_relative_prob").over(w_lead))
        # Calculate posterior probability at each locus
        .withColumn(
            "pics_postprob",
            f.col("pics_relative_prob") / f.col("pics_relative_prob_sum"),
        )
        # Calculate cumulative sum per locus
        .withColumn("pics_postprob_cumsum", f.sum("pics_postprob").over(w_cumlead))
        # Label whether they are in the 95 or 99% credible set
        .withColumn(
            "pics_95perc_credset",
            f.when(
                f.lag("pics_postprob_cumsum", 1).over(w_credset) >= 0.95, False
            ).otherwise(True),
        )
        .withColumn(
            "pics_99perc_credset",
            f.when(
                f.lag("pics_postprob_cumsum", 1).over(w_credset) >= 0.99, False
            ).otherwise(True),
        )
        .select(
            "chromosome",
            "studyAccession",
            "variantId",
            "tagVariantId",
            "R_overall",
            "pics_mu",
            "pics_postprob",
            "pics_95perc_credset",
            "pics_99perc_credset",
        )
    )

    return association_ancestry_ld
