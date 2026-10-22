"""BigBrain fine-mapping ingestion.

BigBrain publishes its own SuSiE fine-mapping alongside the summary statistics
(`BigBrain_cis_eQTL_finemapping.txt.gz`, `BigBrain_cis_sQTL_finemapping.txt.gz`).
These were produced with PolyFun's `finemapper.py` running SuSiE (L=5, residual variance
fixed at 1, a 1 Mb window around the feature) on munged Z-scores and sample sizes, with
in-sample LD from 3,884 European-ancestry BigBrain donors (Réal et al. 2025, Methods). That
LD matches the summary statistics far better than any public reference panel, so the
published credible sets are ingested directly rather than re-fine-mapped.

The file has one row per credible-set member variant:

| Column | Meaning |
| --- | --- |
| `SNP` | rsID |
| `CHR`, `BP` | GRCh38 position |
| `A1`, `A2` | effect and other allele |
| `Z`, `P`, `N` | marginal Z-score, p-value and sample size |
| `PIP` | posterior inclusion probability in any of the SuSiE effects |
| `BETA_MEAN`, `BETA_SD` | SuSiE posterior mean and standard deviation of the effect |
| `CREDIBLE_SET` | credible-set index within the feature (1-5) |
| `feature` | molecular trait (versioned Ensembl gene ID, or Leafcutter intron) |

`BETA_MEAN` and `BETA_SD` are stored as the conditional statistics, as the method's direct
output. Unlike the other SuSiE sources, they are not per effect: PolyFun sums the posterior mean
over all effects, and SuSiE was fitted on Z-scores and N, so they are on the standardised scale
(`BETA_SD·sqrt(N)` is about 1 and `BETA_MEAN` about `Z/sqrt(N)` for single-variant credible sets
in the eQTL release). Their sign gives the direction of effect, which the file has no marginal
beta for. They are not rescaled: no marginal standard error is published, and the sQTL
fine-mapping used random-effects Z-scores, so the fixed-effect standard errors of the full summary
statistics would not convert them exactly either.

This is a deliberate exception to the per-effect convention, and its limitations are known:

- A member's `BETA_MEAN` is `sum_l alpha_l * mu_l`, so it includes the other effects of the
  feature. For most members the credible set's own effect dominates and the sign is that effect's
  direction; for members with a low PIP, another effect can dominate and the sign can be wrong.
- `BETA_SD` is the posterior standard deviation of that sum, not of a single effect.
- Neither is comparable in magnitude with the per-effect, per-allele conditional statistics of the
  other sources. Use them for direction, not for effect size.
- The reported beta stays null, so the colocalisation beta ratio of BigBrain credible sets
  rests on this sign alone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.stats import neglogpval_from_z2, pvalue_from_neglogpval
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_locus import FinemappingMethod
from gentropy.datasource.bigbrain import BigBrainPublicationMetadata

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session


class BigBrainFinemapping:
    """Build gentropy credible sets from BigBrain's published SuSiE fine-mapping."""

    raw_schema: t.StructType = t.StructType(
        [
            t.StructField("SNP", t.StringType(), True),
            t.StructField("CHR", t.StringType(), True),
            t.StructField("BP", t.IntegerType(), True),
            t.StructField("A1", t.StringType(), True),
            t.StructField("A2", t.StringType(), True),
            t.StructField("Z", t.DoubleType(), True),
            t.StructField("N", t.IntegerType(), True),
            t.StructField("P", t.DoubleType(), True),
            t.StructField("PIP", t.DoubleType(), True),
            t.StructField("BETA_MEAN", t.DoubleType(), True),
            t.StructField("BETA_SD", t.DoubleType(), True),
            t.StructField("DISTANCE_FROM_CENTER", t.IntegerType(), True),
            t.StructField("CREDIBLE_SET", t.IntegerType(), True),
            t.StructField("feature", t.StringType(), True),
        ]
    )

    @classmethod
    def read_from_source(cls, session: Session, path: str) -> DataFrame:
        """Read a BigBrain fine-mapping file.

        Args:
            session (Session): Gentropy session.
            path (str): Path to the tab-separated (optionally gzipped) fine-mapping file.

        Returns:
            DataFrame: Raw fine-mapping rows.
        """
        # FAILFAST: in the default PERMISSIVE mode a malformed field silently becomes
        # null, which would drop or corrupt credible-set members without an error.
        return session.spark.read.csv(
            path, sep="\t", header=True, schema=cls.raw_schema, mode="FAILFAST"
        )

    @classmethod
    def from_source(
        cls, finemapping: DataFrame, qtl_type: str
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Convert BigBrain fine-mapping rows into credible sets and their variants.

        - `variantId` is `CHR_BP_A2_A1`, so the effect allele `A1` is the alternate
          allele, matching gentropy's convention. In the EUR eQTL release 99.4% of
          rows match gnomAD-oriented variant IDs this way and none need flipping.
        - Rows with `Z == 0` are placeholders for variants that were not tested, and
          match no gnomAD-oriented variant ID. Credible sets led by one are artefacts
          and are dropped; any remaining such rows are removed.
        - The p-value is recomputed from `Z` in -log10 space. The published `P` is
          fine (the strongest is about 1e-235), but this cannot underflow and keeps
          the mantissa/exponent split consistent with gentropy's other sources.
        - `sampleSize` is `N`, the sample size of the association test at the locus.
        - The published credible sets are kept as they are: PolyFun reports SuSiE's 95% credible
          sets, so no member is dropped by a cumulative PIP cut.
        - `posteriorProbability` is `PIP` scaled so that it sums to 0.9501 over each credible set.
          The published `PIP` is the probability of being causal in any of the SuSiE effects, not
          in the credible set's own effect (the per-effect probabilities are not published), and
          each variant is listed in one credible set only, so the published sums range from 0.03
          to 2.6. Scaling keeps the order of the members; for the few sets that lost variants to
          an earlier set of their feature, it inflates the members that remain.
        - `conditionalBeta` and `conditionalStandardError` are `BETA_MEAN` and `BETA_SD`, summed
          over all effects and on the standardised scale (see the module docstring). The reported
          beta and standard error are null, as the file has none, and so are the Bayes factors,
          which are not published.
        - The lead of a credible set is the top PIP, ties broken by variant id, and its bounds
          are those of its members.
        - Purity is not published either. SuSiE only reports sets with a minimum
          absolute correlation of 0.5 on the in-sample LD, so every set passed a
          purity filter at source; purity is set to 1 for single-variant sets and
          left null otherwise.

        Args:
            finemapping (DataFrame): Raw rows from `read_from_source`.
            qtl_type (str): QTL type, e.g. `StudyType.EQTL.value`.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: BigBrain credible sets and their variants.
        """
        pub = BigBrainPublicationMetadata()
        credible_set = Window.partitionBy("studyId", "credibleSetIndex")
        by_pip = credible_set.orderBy(
            f.desc("posteriorProbability"), f.col("variantId")
        )

        variants = (
            finemapping.select(
                f.concat_ws(
                    "_",
                    f.lit("BigBrain"),
                    f.lit(qtl_type),
                    f.lit(pub.ANCESTRY),
                    f.col("feature"),
                ).alias("studyId"),
                f.col("CHR").alias("chromosome"),
                f.col("BP").alias("position"),
                f.concat_ws(
                    "_", f.col("CHR"), f.col("BP"), f.col("A2"), f.col("A1")
                ).alias("variantId"),
                f.col("Z").alias("zScore"),
                f.col("N").alias("sampleSize"),
                f.col("PIP").alias("posteriorProbability"),
                f.col("BETA_MEAN").alias("conditionalBeta"),
                f.col("BETA_SD").alias("conditionalStandardError"),
                f.col("CREDIBLE_SET").alias("credibleSetIndex"),
            )
            .withColumn("isLead", f.row_number().over(by_pip) == 1)
            # Drop credible sets whose lead is an untested placeholder, then any
            # remaining placeholder rows.
            .withColumn(
                "leadIsUntested",
                f.max(f.when(f.col("isLead") & (f.col("zScore") == 0), True)).over(
                    credible_set
                ),
            )
            .filter(f.col("leadIsUntested").isNull() & (f.col("zScore") != 0))
            .drop("leadIsUntested")
            .select(
                "*", *pvalue_from_neglogpval(neglogpval_from_z2(f.col("zScore") ** 2))
            )
        )
        single_variant_purity = f.when(
            f.count("variantId").over(credible_set) == 1, f.lit(1.0)
        ).cast(t.DoubleType())
        members = (
            variants.withColumn(
                "posteriorProbability",
                f.col("posteriorProbability")
                * 0.9501
                / f.sum("posteriorProbability").over(credible_set),
            )
            .withColumns(
                {
                    "studyType": f.lit(qtl_type),
                    "fineMappingMethod": f.lit(FinemappingMethod.SUSIE.value),
                    # Bounds and purity describe the tested members.
                    "locusStart": f.min("position").over(credible_set),
                    "locusEnd": f.max("position").over(credible_set),
                }
            )
            .withColumns(
                {
                    "purityMeanR2": single_variant_purity,
                    "purityMinR2": single_variant_purity,
                }
            )
        )
        return CredibleSet.from_members(members, ["studyId", "credibleSetIndex"])
