"""BigBrain fine-mapping ingestion.

BigBrain publishes its own SuSiE fine-mapping alongside the summary statistics
(`BigBrain_cis_eQTL_finemapping.txt.gz`, `BigBrain_cis_sQTL_finemapping.txt.gz`).
These were produced with PolyFun + SuSiE (L=5, residual variance fixed at 1) using
in-sample LD from 3,884 European-ancestry BigBrain donors, on the EUR meta-analysis.
That LD matches the summary statistics far better than any public reference panel,
so the published credible sets are ingested directly rather than re-fine-mapped.

The file has one row per credible-set member variant:

| Column | Meaning |
| --- | --- |
| `SNP` | rsID |
| `CHR`, `BP` | GRCh38 position |
| `A1`, `A2` | effect and other allele |
| `Z`, `P`, `N` | marginal Z-score, p-value and sample size |
| `PIP` | posterior inclusion probability |
| `BETA_MEAN`, `BETA_SD` | SuSiE posterior mean and standard deviation of the effect |
| `CREDIBLE_SET` | credible-set index within the feature (1-5) |
| `feature` | molecular trait (versioned Ensembl gene ID, or Leafcutter intron) |
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.stats import neglogpval_from_z2, pvalue_from_neglogpval
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
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
    def from_source(cls, finemapping: DataFrame, qtl_type: str) -> StudyLocus:
        """Convert BigBrain fine-mapping rows into credible sets.

        - `variantId` is `CHR_BP_A2_A1`, so the effect allele `A1` is the alternate
          allele, matching gentropy's convention. In the EUR eQTL release 99.4% of
          rows match the gnomAD-harmonised summary statistics this way and none
          need flipping.
        - Rows with `Z == 0` are placeholders for variants that were not tested, and
          are absent from the harmonised summary statistics. Credible sets led by
          one are artefacts and are dropped; any remaining such rows are removed.
        - The p-value is recomputed from `Z` in -log10 space. The published `P` is
          fine (the strongest is about 1e-235), but this cannot underflow and keeps
          the mantissa/exponent split consistent with gentropy's other sources.
        - `logBF` is not published, so it is left null. PIP-based colocalisation
          methods do not need it.
        - Purity is not published either. SuSiE only reports sets with a minimum
          absolute correlation of 0.5 on the in-sample LD, so every set passed a
          purity filter at source; purity is set to 1 for single-variant sets and
          left null otherwise.

        Args:
            finemapping (DataFrame): Raw rows from `read_from_source`.
            qtl_type (str): QTL type, e.g. `BigBrainQtlType.EQTL.value`.

        Returns:
            StudyLocus: BigBrain credible sets.
        """
        pub = BigBrainPublicationMetadata()
        credible_set = Window.partitionBy("studyId", "credibleSetIndex")
        by_pip = credible_set.orderBy(f.desc("posteriorProbability"), f.col("variantId"))

        variants = (
            finemapping.select(
                f.concat_ws(
                    "_", f.lit("BigBrain"), f.lit(qtl_type), f.lit(pub.ANCESTRY), f.col("feature")
                ).alias("studyId"),
                f.col("CHR").alias("chromosome"),
                f.col("BP").alias("position"),
                f.concat_ws("_", f.col("CHR"), f.col("BP"), f.col("A2"), f.col("A1")).alias(
                    "variantId"
                ),
                f.col("Z").alias("zScore"),
                f.col("N").alias("sampleSize"),
                f.col("PIP").alias("posteriorProbability"),
                f.col("BETA_MEAN").alias("beta"),
                f.col("BETA_SD").alias("standardError"),
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
            .select("*", *pvalue_from_neglogpval(neglogpval_from_z2(f.col("zScore") ** 2)))
        )

        locus_struct = f.struct(
            "variantId",
            "posteriorProbability",
            "pValueMantissa",
            "pValueExponent",
            f.lit(None).cast(t.DoubleType()).alias("logBF"),
            "beta",
            "standardError",
        )
        credible_sets = (
            variants.withColumn("locus", f.collect_list(locus_struct).over(credible_set))
            .withColumn("locusStart", f.min("position").over(credible_set))
            .withColumn("locusEnd", f.max("position").over(credible_set))
            .filter(f.col("isLead"))
            .withColumn("finemappingMethod", f.lit(FinemappingMethod.SUSIE.value))
            .withColumn(
                "purity", f.when(f.size("locus") == 1, f.lit(1.0)).cast(t.DoubleType())
            )
            .select(
                StudyLocus.assign_study_locus_id(
                    ["studyId", "variantId", "finemappingMethod"]
                ),
                "studyId",
                f.lit(qtl_type).alias("studyType"),
                "variantId",
                "chromosome",
                "position",
                f.concat(
                    f.col("chromosome"),
                    f.lit(":"),
                    f.col("locusStart"),
                    f.lit("-"),
                    f.col("locusEnd"),
                ).alias("region"),
                "beta",
                "zScore",
                "pValueMantissa",
                "pValueExponent",
                "standardError",
                "finemappingMethod",
                "credibleSetIndex",
                f.col("purity").alias("purityMeanR2"),
                f.col("purity").alias("purityMinR2"),
                "locusStart",
                "locusEnd",
                "sampleSize",
                "locus",
                f.array().cast(t.ArrayType(t.StringType(), False)).alias(
                    "qualityControls"
                ),
                f.lit(False).alias("isTransQtl"),
            )
        )
        return (
            StudyLocus(_df=credible_sets, _schema=StudyLocus.get_schema())
            .annotate_credible_sets()
            .assign_confidence()
        )
