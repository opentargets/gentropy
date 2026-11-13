# pylint: disable=unsubscriptable-object
"""Datasource ingestion: FinnGen fine-mapping results (SuSiE) to credible sets."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from gentropy.common.stats import (
    region_log_bayes_factors,
    split_pvalue_column,
)
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_locus import FinemappingMethod


def _effect(column_prefix: str) -> Column:
    """The value of a per-effect column for the credible set of the row.

    SuSiE reports up to 10 single effects as `<prefix>1` .. `<prefix>10`; the credible set index
    `cs` says which effect the row's credible set is.

    Args:
        column_prefix (str): Column name before the effect number, e.g. `alpha`.

    Returns:
        Column: The value of the credible set's effect.
    """
    return f.element_at(
        f.array(*[f.col(f"{column_prefix}{i}") for i in range(1, 11)]),
        f.col("credibleSetIndex"),
    )


@dataclass
class FinnGenFinemapping:
    """SuSiE fine-mapping results of FinnGen.

    The `*.SUSIE.snp.bgz` files hold every variant of each fine-mapped region with its marginal
    statistics and its SuSiE single effect statistics; the `*.SUSIE.cred.summary.tsv` files hold
    one row per credible set. They are turned into credible sets and their variants:

    - The study identifier is the release prefix and the trait (e.g. `FINNGEN_R12_AB1_EBV`).
    - `allele1` and `allele2` are the reference and alternative alleles of the variant identifier
      `v` (`chr:pos:ref:alt`, GRCh38), and `beta` refers to the alternative allele. The FinnGen
      documentation calls them major and minor alleles, but the fine-mapping pipeline is run with
      `allele1_col=ref` and `allele2_col=alt` (FINNGEN/finemapping-pipeline).
    - `maf` is the alternative allele frequency folded at 0.5, so it is not the effect allele
      frequency when the alternative allele is the major one. The effect allele frequency is the
      alternative allele frequency `AF` of the FinnGen annotated variants file instead.
    - The source gives no per-variant sample size of the marginal test, so `sampleSize` is null.

    The fine-mapping method is SuSiE.
    """

    raw_schema: t.StructType = StructType(
        [
            StructField("trait", StringType(), True),
            StructField("region", StringType(), True),
            StructField("v", StringType(), True),
            StructField("rsid", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", StringType(), True),
            StructField("allele1", StringType(), True),
            StructField("allele2", StringType(), True),
            StructField("maf", StringType(), True),
            StructField("beta", StringType(), True),
            StructField("se", StringType(), True),
            StructField("p", StringType(), True),
            StructField("mean", StringType(), True),
            StructField("sd", StringType(), True),
            StructField("prob", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("cs_specific_prob", DoubleType(), True),
            StructField("low_purity", StringType(), True),
            StructField("lead_r2", StringType(), True),
            StructField("mean_99", StringType(), True),
            StructField("sd_99", StringType(), True),
            StructField("prob_99", StringType(), True),
            StructField("cs_99", StringType(), True),
            StructField("cs_specific_prob_99", StringType(), True),
            StructField("low_purity_99", StringType(), True),
            StructField("lead_r2_99", StringType(), True),
            StructField("alpha1", DoubleType(), True),
            StructField("alpha2", DoubleType(), True),
            StructField("alpha3", DoubleType(), True),
            StructField("alpha4", DoubleType(), True),
            StructField("alpha5", DoubleType(), True),
            StructField("alpha6", DoubleType(), True),
            StructField("alpha7", DoubleType(), True),
            StructField("alpha8", DoubleType(), True),
            StructField("alpha9", DoubleType(), True),
            StructField("alpha10", DoubleType(), True),
            StructField("mean1", StringType(), True),
            StructField("mean2", StringType(), True),
            StructField("mean3", StringType(), True),
            StructField("mean4", StringType(), True),
            StructField("mean5", StringType(), True),
            StructField("mean6", StringType(), True),
            StructField("mean7", StringType(), True),
            StructField("mean8", StringType(), True),
            StructField("mean9", StringType(), True),
            StructField("mean10", StringType(), True),
            StructField("sd1", StringType(), True),
            StructField("sd2", StringType(), True),
            StructField("sd3", StringType(), True),
            StructField("sd4", StringType(), True),
            StructField("sd5", StringType(), True),
            StructField("sd6", StringType(), True),
            StructField("sd7", StringType(), True),
            StructField("sd8", StringType(), True),
            StructField("sd9", StringType(), True),
            StructField("sd10", StringType(), True),
            StructField("lbf_variable1", DoubleType(), True),
            StructField("lbf_variable2", DoubleType(), True),
            StructField("lbf_variable3", DoubleType(), True),
            StructField("lbf_variable4", DoubleType(), True),
            StructField("lbf_variable5", DoubleType(), True),
            StructField("lbf_variable6", DoubleType(), True),
            StructField("lbf_variable7", DoubleType(), True),
            StructField("lbf_variable8", DoubleType(), True),
            StructField("lbf_variable9", DoubleType(), True),
            StructField("lbf_variable10", DoubleType(), True),
        ]
    )

    summary_schema: t.StructType = StructType(
        [
            StructField("trait", StringType(), True),
            StructField("region", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("cs_log10bf", DoubleType(), True),
            StructField("cs_avg_r2", DoubleType(), True),
            StructField("cs_min_r2", DoubleType(), True),
        ]
    )

    @classmethod
    def from_finngen_susie_finemapping(
        cls: type[FinnGenFinemapping],
        spark: SparkSession,
        finngen_susie_finemapping_snp_files: str | list[str],
        finngen_susie_finemapping_cs_summary_files: str | list[str],
        finngen_annotated_variants_path: str,
        finngen_release_prefix: str,
        credset_lbf_threshold: float = 0.8685889638065036,
        lead_pvalue_threshold: float | None = None,
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Process the SuSiE fine-mapping outputs of FinnGen into credible sets and their variants.

        The block gzipped (`.bgz`) and gzipped files are read with Spark, which needs a session with
        `use_enhanced_bgzip_codec`.

        A credible set is a SuSiE single effect `cs` of a fine-mapped `region` of a trait. Its
        members are the variants FinnGen assigns to it (`cs` above 0, 95% coverage). The credible set
        summaries drop the credible sets whose `cs_log10bf` is not above `credset_lbf_threshold`,
        except the first one of a region. `cs_log10bf` compares the model with `cs` credible sets to
        the one with `cs - 1`; it is only used for this filter and is not stored.

        Credible set columns:

        - `leadVariantId`: the member with the highest posterior probability, ties broken by variant id.
        - `locusStart`, `locusEnd`: the bounds of the fine-mapped region.
        - `logBayesFactor`: the region-wide Bayes factor of the single effect, computed from every
          variant of the region (see `gentropy.common.stats.region_log_bayes_factors`).
        - `purityMeanR2`, `purityMinR2`: `cs_avg_r2` and `cs_min_r2` of the credible set summaries.

        Credible set variant columns:

        - `pValueMantissa`, `pValueExponent`, `reportedBeta`, `reportedStandardError`: the marginal
          `p`, `beta` and `se`.
        - `posteriorProbability`: the variant's posterior inclusion probability in the single
          effect, `alpha<cs>`.
        - `logBayesFactor`: the variant's natural log Bayes factor in the single effect,
          `lbf_variable<cs>`.
        - `conditionalBeta`, `conditionalStandardError`: the posterior mean and standard deviation
          of the variant's effect given that it is the causal variant of the single effect,
          `mu<cs>` and `sqrt(mu2<cs> - mu<cs>^2)` in SuSiE's notation, per allele. FinnGen reports
          them weighted by the posterior probability, `mean<cs> = alpha<cs> * mu<cs>` and
          `sd<cs> = sqrt(alpha<cs> * mu2<cs> - mean<cs>^2)` (the overall `mean` is their sum over
          effects), so they are recovered exactly as `mean<cs> / alpha<cs>` and
          `sqrt((sd<cs>^2 + mean<cs>^2) / alpha<cs> - (mean<cs> / alpha<cs>)^2)`. This makes them the
          same quantity as the other SuSiE sources' conditional statistics.
        - `r2Overall`: `lead_r2`, the in-sample r2 with FinnGen's lead of the credible set. FinnGen's
          lead is the variant with the highest overall posterior inclusion probability (`prob`),
          which is nearly always our lead, the highest `alpha<cs>`.
        - `effectAlleleFrequency`: the alternative allele frequency `AF` of the annotated variants
          file, flagged `EAF_FROM_SOURCE`; null when the variant is not in it.

        Args:
            spark (SparkSession): SparkSession object.
            finngen_susie_finemapping_snp_files (str | list[str]): SuSiE fine-mapping variant files (`*.SUSIE.snp.bgz`).
            finngen_susie_finemapping_cs_summary_files (str | list[str]): SuSiE credible set summaries (`*.SUSIE.cred.summary.tsv`).
            finngen_annotated_variants_path (str): FinnGen annotated variants file of the same release.
            finngen_release_prefix (str): FinnGen project release prefix. Should look like FINNGEN_R*.
            credset_lbf_threshold (float): Smallest `cs_log10bf` of a credible set other than the first of its region.
                Defaults to 0.8685889638065036, log10(exp(2)), the threshold of the FinnGen publication.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above it.
                Defaults to None, which keeps every credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: FinnGen credible sets and their variants.
        """
        snps = (
            spark.read.schema(cls.raw_schema)
            .option("delimiter", "\t")
            .csv(finngen_susie_finemapping_snp_files, header=True)
            # Drop rows which don't have proper position.
            .filter(f.col("position").cast(t.IntegerType()).isNotNull())
            .withColumn(
                "studyId",
                f.concat_ws("_", f.lit(finngen_release_prefix), f.col("trait")),
            )
        )
        credible_sets = (
            spark.read.schema(cls.summary_schema)
            .option("delimiter", "\t")
            .csv(finngen_susie_finemapping_cs_summary_files, header=True)
            .select(
                f.concat_ws("_", f.lit(finngen_release_prefix), f.col("trait")).alias(
                    "studyId"
                ),
                f.col("region"),
                f.col("cs").cast(t.IntegerType()).alias("credibleSetIndex"),
                f.col("cs_log10bf"),
                f.col("cs_avg_r2").alias("purityMeanR2"),
                f.col("cs_min_r2").alias("purityMinR2"),
            )
            .filter(
                (f.col("cs_log10bf") > credset_lbf_threshold)
                | (f.col("credibleSetIndex") == 1)
            )
            .drop("cs_log10bf")
        )
        # Every variant of the region counts, not only the credible set members:
        region_bayes_factors = region_log_bayes_factors(
            snps.select(
                "studyId", "region", *[f"lbf_variable{i}" for i in range(1, 11)]
            ).join(
                credible_sets.select("studyId", "region", "credibleSetIndex"),
                on=["studyId", "region"],
            ),
            ["studyId", "region", "credibleSetIndex"],
            _effect("lbf_variable").cast(t.DoubleType()),
        )
        # Read twice, to select the allele frequencies and to join them: persist, so the
        # SuSiE outputs are read once.
        members = (
            snps.filter(f.col("cs").cast(t.IntegerType()) > 0)
            .withColumn("credibleSetIndex", f.col("cs").cast(t.IntegerType()))
            .join(credible_sets, on=["studyId", "region", "credibleSetIndex"])
            .join(region_bayes_factors, on=["studyId", "region", "credibleSetIndex"])
            .persist()
        )
        allele_frequencies = (
            spark.read.option("delimiter", "\t")
            .csv(finngen_annotated_variants_path, header=True)
            .select(f.col("#variant").alias("v"), f.col("AF").cast(t.FloatType()))
            # Only the frequencies of the members are needed. Spark broadcasts the members
            # when they are few enough, and shuffles them otherwise:
            .join(members.select("v").distinct(), on="v")
        )
        effect_allele_frequency = f.col("AF")
        alpha = _effect("alpha").cast(t.DoubleType())
        mean = _effect("mean").cast(t.DoubleType())
        sd = _effect("sd").cast(t.DoubleType())
        # FinnGen reports the effect weighted by alpha; dividing by it gives the conditional
        # moments. Clamped at 0, as the variance can come out slightly negative in floating point:
        conditional_beta = f.when(alpha > 0, mean / alpha)
        conditional_variance = (sd**2 + mean**2) / alpha - conditional_beta**2
        members = members.join(allele_frequencies, on="v", how="left").select(
            "studyId",
            "region",
            "credibleSetIndex",
            f.regexp_replace(f.col("v"), ":", "_").alias("variantId"),
            f.regexp_replace(f.col("chromosome"), "^chr", "").alias("chromosome"),
            f.split(f.split("region", ":")[1], "-")[0]
            .cast(t.IntegerType())
            .alias("locusStart"),
            f.split(f.split("region", ":")[1], "-")[1]
            .cast(t.IntegerType())
            .alias("locusEnd"),
            f.lit("gwas").alias("studyType"),
            f.lit(FinemappingMethod.SUSIE.value).alias("fineMappingMethod"),
            "credibleSetLogBayesFactor",
            "purityMeanR2",
            "purityMinR2",
            *split_pvalue_column(f.col("p").cast(t.DoubleType())),
            f.col("beta").cast(t.DoubleType()).alias("reportedBeta"),
            f.col("se").cast(t.DoubleType()).alias("reportedStandardError"),
            effect_allele_frequency.alias("effectAlleleFrequency"),
            _effect("alpha").alias("posteriorProbability"),
            _effect("lbf_variable").alias("logBayesFactor"),
            conditional_beta.alias("conditionalBeta"),
            f.when(
                alpha > 0, f.sqrt(f.greatest(conditional_variance, f.lit(0.0)))
            ).alias("conditionalStandardError"),
            f.col("lead_r2").cast(t.FloatType()).alias("r2Overall"),
            f.when(
                effect_allele_frequency.isNotNull(),
                f.array(f.lit(CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value)),
            )
            .otherwise(f.array())
            .alias("qualityControls"),
        )
        return CredibleSet.from_members(
            members,
            ["studyId", "region", "credibleSetIndex"],
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
