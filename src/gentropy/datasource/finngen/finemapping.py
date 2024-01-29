# pylint: disable=unsubscriptable-object
"""Datasource ingestion: FinnGen Finemapping results (SuSIE) to studyLocus object."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StringType, StructField, StructType

from gentropy.common.spark_helpers import get_top_ranked_in_window
from gentropy.common.utils import parse_pvalue
from gentropy.dataset.study_locus import StudyLocus


@dataclass
class FinnGenFinemapping:
    """SuSIE finemapping dataset for FinnGen.

    Credible sets from SuSIE are extracted and transformed into StudyLocus objects:

    - Study ID in the special format (e.g. FINNGEN_R10_*)
    - Credible set specific finemapping statistics (e.g. LogBayesFactors, Alphas/Posterior)
    - Additional credible set level BayesFactor filtering is applied (LBF > 2)
    - StudyLocusId is annotated for each credible set.

    Finemapping method is populated as a constant ("SuSIE").
    """

    finngen_release_prefix: str = "FINNGEN_R10"
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
            StructField("alpha1", StringType(), True),
            StructField("alpha2", StringType(), True),
            StructField("alpha3", StringType(), True),
            StructField("alpha4", StringType(), True),
            StructField("alpha5", StringType(), True),
            StructField("alpha6", StringType(), True),
            StructField("alpha7", StringType(), True),
            StructField("alpha8", StringType(), True),
            StructField("alpha9", StringType(), True),
            StructField("alpha10", StringType(), True),
            StructField("lbf_variable1", StringType(), True),
            StructField("lbf_variable2", StringType(), True),
            StructField("lbf_variable3", StringType(), True),
            StructField("lbf_variable4", StringType(), True),
            StructField("lbf_variable5", StringType(), True),
            StructField("lbf_variable6", StringType(), True),
            StructField("lbf_variable7", StringType(), True),
            StructField("lbf_variable8", StringType(), True),
            StructField("lbf_variable9", StringType(), True),
            StructField("lbf_variable10", StringType(), True),
        ]
    )

    summary_schema: t.StructType = StructType(
        [
            StructField("trait", StringType(), True),
            StructField("region", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("cs_log10bf", StringType(), True),
        ]
    )

    @classmethod
    def from_finngen_susie_finemapping(
        cls: type[FinnGenFinemapping],
        spark: SparkSession,
        finngen_finemapping_df: (str | list[str]),
        finngen_finemapping_summaries: (str | list[str]),
        credset_lbf_threshold: float = 0.8685889638065036,
    ) -> StudyLocus:
        """Process the SuSIE finemapping output for FinnGen studies.

        Args:
            spark (SparkSession): Spark session object.
            finngen_finemapping_df (str | list[str]): SuSIE finemapping output filename(s).
            finngen_finemapping_summaries (str | list[str]): filename of SuSIE finemapping summaries.
            credset_lbf_threshold (float, optional): Filter out credible sets below, Default 0.8685889638065036 == np.log10(np.exp(2)), this is threshold from publication.

        Returns:
            StudyLocus: Processed SuSIE finemapping output in StudyLocus format.
        """
        processed_finngen_finemapping_df = (
            spark.read.schema(cls.raw_schema)
            .option("delimiter", "\t")
            .option("compression", "gzip")
            .csv(finngen_finemapping_df, header=True)
            # Drop rows which don't have proper position.
            .filter(f.col("position").cast(t.IntegerType()).isNotNull())
            # Drop non credible set SNPs:
            .filter(f.col("cs").cast(t.IntegerType()) > 0)
            .select(
                # Add study idenfitier.
                f.concat(f.lit(cls.finngen_release_prefix), f.col("trait"))
                .cast(t.StringType())
                .alias("studyId"),
                f.col("region"),
                # Add variant information.
                f.regexp_replace(f.col("v"), ":", "_").alias("variantId"),
                f.col("cs").cast("integer").alias("credibleSetIndex"),
                f.regexp_replace(f.col("chromosome"), "^chr", "")
                .cast(t.StringType())
                .alias("chromosome"),
                f.col("position").cast(t.IntegerType()),
                f.col("allele1").cast(t.StringType()).alias("ref"),
                f.col("allele2").cast(t.StringType()).alias("alt"),
                # Parse p-value into mantissa and exponent.
                *parse_pvalue(f.col("p")),
                # Add beta, standard error, and allele frequency information.
                f.col("beta").cast("double"),
                f.col("se").cast("double").alias("standardError"),
                f.col("maf").cast("float").alias("effectAlleleFrequencyFromSource"),
                f.lit("SuSie").cast("string").alias("finemappingMethod"),
                *[
                    f.col(f"alpha{i}").cast(t.DoubleType()).alias(f"alpha_{i}")
                    for i in range(1, 11)
                ],
                *[
                    f.col(f"lbf_variable{i}").cast(t.DoubleType()).alias(f"lbf_{i}")
                    for i in range(1, 11)
                ],
            )
            .withColumn(
                "posteriorProbability",
                f.when(f.col("credibleSetIndex") == 1, f.col("alpha_1"))
                .when(f.col("credibleSetIndex") == 2, f.col("alpha_2"))
                .when(f.col("credibleSetIndex") == 3, f.col("alpha_3"))
                .when(f.col("credibleSetIndex") == 4, f.col("alpha_4"))
                .when(f.col("credibleSetIndex") == 5, f.col("alpha_5"))
                .when(f.col("credibleSetIndex") == 6, f.col("alpha_6"))
                .when(f.col("credibleSetIndex") == 7, f.col("alpha_7"))
                .when(f.col("credibleSetIndex") == 8, f.col("alpha_8"))
                .when(f.col("credibleSetIndex") == 9, f.col("alpha_9"))
                .when(f.col("credibleSetIndex") == 10, f.col("alpha_10")),
            )
            .drop(
                "alpha_1",
                "alpha_2",
                "alpha_3",
                "alpha_4",
                "alpha_5",
                "alpha_6",
                "alpha_7",
                "alpha_8",
                "alpha_9",
                "alpha_10",
            )
            .withColumn(
                "logBF",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_1"))
                .when(f.col("credibleSetIndex") == 2, f.col("lbf_2"))
                .when(f.col("credibleSetIndex") == 3, f.col("lbf_3"))
                .when(f.col("credibleSetIndex") == 4, f.col("lbf_4"))
                .when(f.col("credibleSetIndex") == 5, f.col("lbf_5"))
                .when(f.col("credibleSetIndex") == 6, f.col("lbf_6"))
                .when(f.col("credibleSetIndex") == 7, f.col("lbf_7"))
                .when(f.col("credibleSetIndex") == 8, f.col("lbf_8"))
                .when(f.col("credibleSetIndex") == 9, f.col("lbf_9"))
                .when(f.col("credibleSetIndex") == 10, f.col("lbf_10")),
            )
            .drop(
                "lbf_1",
                "lbf_2",
                "lbf_3",
                "lbf_4",
                "lbf_5",
                "lbf_6",
                "lbf_7",
                "lbf_8",
                "lbf_9",
                "lbf_10",
            )
        )

        # drop credible sets where logbf > 2. Except when there's only one credible set in region:
        # 0.8685889638065036 corresponds to np.log10(np.exp(2)), to match the orginal threshold in publication.
        finngen_finemapping_summaries_df = (
            # Read credible set level lbf, it is output as a different file which is not ideal.
            spark.read.schema(cls.summary_schema)
            .option("delimiter", "\t")
            .csv(finngen_finemapping_summaries, header=True)
            .select(
                f.col("region"),
                f.col("trait"),
                f.col("cs").cast("integer").alias("credibleSetIndex"),
                f.col("cs_log10bf").cast("double").alias("credibleSetlog10BF"),
            )
            .filter(
                (f.col("credibleSetlog10BF") > credset_lbf_threshold)
                | (f.col("credibleSetIndex") == 1)
            )
            .withColumn(
                "studyId", f.concat(f.lit(cls.finngen_release_prefix), f.col("trait"))
            )
        )

        processed_finngen_finemapping_df = processed_finngen_finemapping_df.join(
            finngen_finemapping_summaries_df,
            on=["studyId", "region", "credibleSetIndex"],
            how="inner",
        )

        toploci_df = get_top_ranked_in_window(
            processed_finngen_finemapping_df,
            Window.partitionBy("studyId", "region", "credibleSetIndex").orderBy(
                *[
                    f.col("pValueExponent").asc(),
                    f.col("pValueMantissa").asc(),
                ]
            ),
        ).select(
            "variantId",
            "chromosome",
            "position",
            "studyId",
            "beta",
            "pValueMantissa",
            "pValueExponent",
            "effectAlleleFrequencyFromSource",
            "standardError",
            "region",
            "credibleSetIndex",
            "finemappingMethod",
            "credibleSetlog10BF",
        )

        processed_finngen_finemapping_df = (
            processed_finngen_finemapping_df.groupBy(
                "studyId", "region", "credibleSetIndex"
            )
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("variantId").cast("string").alias("variantId"),
                        f.col("posteriorProbability")
                        .cast("double")
                        .alias("posteriorProbability"),
                        f.col("logBF").cast("double").alias("logBF"),
                        f.col("pValueMantissa").cast("float").alias("pValueMantissa"),
                        f.col("pValueExponent").cast("integer").alias("pValueExponent"),
                        f.col("beta").cast("double").alias("beta"),
                        f.col("standardError").cast("double").alias("standardError"),
                    )
                ).alias("locus"),
            )
            .select(
                "studyId",
                "region",
                "credibleSetIndex",
                "locus",
            )
            .join(
                toploci_df,
                on=["studyId", "region", "credibleSetIndex"],
                how="inner",
            )
        ).withColumn(
            "studyLocusId",
            StudyLocus.assign_study_locus_id(f.col("studyId"), f.col("variantId")),
        )

        return StudyLocus(
            _df=processed_finngen_finemapping_df,
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()
