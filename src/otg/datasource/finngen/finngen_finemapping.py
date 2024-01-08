"""Datasource ingestion: FinnGen Finemapping results (SuSIE) to studyLocus object."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from otg.common.utils import parse_pvalue
from otg.dataset.summary_statistics import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


@dataclass
class FinnGenFinemapping(SummaryStatistics):
    """SuSIE finemapping dataset for FinnGen."""

    @classmethod
    def from_finngen_susie_finemapping(
        cls: type[FinnGenFinemapping],
        finngen_finemapping_df: DataFrame,
        finngen_finemapping_summaries: DataFrame,
        finngen_release_prefix: str,
    ) -> FinnGenFinemapping:
        """Process the SuSIE finemapping output for FinnGen studies.

        Args:
            finngen_finemapping_df (DataFrame): SuSIE finemapping output for one FinnGen study.
            finngen_finemapping_summaries (DataFrame): SuSIE finemapping summaries for one FinnGen study.
            finngen_release_prefix (str): FinnGen study prefix.

        Returns:
            FinnGenFinemapping: Processed SuSIE finemapping output in StudyLocus format.
        """
        processed_finngen_finemapping_df = (
            finngen_finemapping_df
            # Drop rows which don't have proper position.
            .filter(f.col("position").cast(t.IntegerType()).isNotNull())
            # Drop non credible set SNPs:
            .filter(f.col("cs") > 0)
            .select(
                # Add study idenfitier.
                f.concat(f.lit(finngen_release_prefix), f.col("trait"))
                .cast(t.StringType())
                .alias("studyId"),
                f.col("region"),
                # Add variant information.
                f.regexp_replace(f.col("v"), ":", "_").alias("variantId"),
                f.col("prob").alias("posteriorProbability"),
                f.col("cs").alias("credibleSetIndex"),
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
                f.col("alpha1").cast("double").alias("alpha_1"),
                f.col("alpha2").cast("double").alias("alpha_2"),
                f.col("alpha3").cast("double").alias("alpha_3"),
                f.col("alpha4").cast("double").alias("alpha_4"),
                f.col("alpha5").cast("double").alias("alpha_5"),
                f.col("alpha6").cast("double").alias("alpha_6"),
                f.col("alpha7").cast("double").alias("alpha_7"),
                f.col("alpha8").cast("double").alias("alpha_8"),
                f.col("alpha9").cast("double").alias("alpha_9"),
                f.col("alpha10").cast("double").alias("alpha_10"),
                f.col("lbf_variable1").cast("double").alias("lbf_1"),
                f.col("lbf_variable2").cast("double").alias("lbf_2"),
                f.col("lbf_variable3").cast("double").alias("lbf_3"),
                f.col("lbf_variable4").cast("double").alias("lbf_4"),
                f.col("lbf_variable5").cast("double").alias("lbf_5"),
                f.col("lbf_variable6").cast("double").alias("lbf_6"),
                f.col("lbf_variable7").cast("double").alias("lbf_7"),
                f.col("lbf_variable8").cast("double").alias("lbf_8"),
                f.col("lbf_variable9").cast("double").alias("lbf_9"),
                f.col("lbf_variable10").cast("double").alias("lbf_10"),
            )
            .withColumn(
                "posteriorProbability",
                f.when(f.col("credibleSetIndex") == 1, f.col("alpha_1")).otherwise(
                    f.when(f.col("credibleSetIndex") == 2, f.col("alpha_2")).otherwise(
                        f.when(
                            f.col("credibleSetIndex") == 3, f.col("alpha_3")
                        ).otherwise(
                            f.when(
                                f.col("credibleSetIndex") == 4, f.col("alpha_4")
                            ).otherwise(
                                f.when(
                                    f.col("credibleSetIndex") == 5, f.col("alpha_5")
                                ).otherwise(
                                    f.when(
                                        f.col("credibleSetIndex") == 6, f.col("alpha_6")
                                    ).otherwise(
                                        f.when(
                                            f.col("credibleSetIndex") == 7,
                                            f.col("alpha_7"),
                                        ).otherwise(
                                            f.when(
                                                f.col("credibleSetIndex") == 8,
                                                f.col("alpha_8"),
                                            ).otherwise(
                                                f.when(
                                                    f.col("credibleSetIndex") == 9,
                                                    f.col("alpha_9"),
                                                ).otherwise(
                                                    f.when(
                                                        f.col("credibleSetIndex") == 10,
                                                        f.col("alpha_10"),
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
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
                "lbf",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_1")).otherwise(
                    f.when(f.col("credibleSetIndex") == 2, f.col("lbf_2")).otherwise(
                        f.when(
                            f.col("credibleSetIndex") == 3, f.col("lbf_3")
                        ).otherwise(
                            f.when(
                                f.col("credibleSetIndex") == 4, f.col("lbf_4")
                            ).otherwise(
                                f.when(
                                    f.col("credibleSetIndex") == 5, f.col("lbf_5")
                                ).otherwise(
                                    f.when(
                                        f.col("credibleSetIndex") == 6, f.col("lbf_6")
                                    ).otherwise(
                                        f.when(
                                            f.col("credibleSetIndex") == 7,
                                            f.col("lbf_7"),
                                        ).otherwise(
                                            f.when(
                                                f.col("credibleSetIndex") == 8,
                                                f.col("lbf_8"),
                                            ).otherwise(
                                                f.when(
                                                    f.col("credibleSetIndex") == 9,
                                                    f.col("lbf_9"),
                                                ).otherwise(
                                                    f.when(
                                                        f.col("credibleSetIndex") == 10,
                                                        f.col("lbf_10"),
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                ),
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

        # drop credible sets where lbf < 2. Except when there's only one credible set in region:

        finngen_finemapping_summaries = (
            finngen_finemapping_summaries.filter(
                (f.col("cs_log10bf") > 2) | (f.col("cs") == 1)
            )
            .withColumn("studyId", f.concat(f.lit("finngen_r9_"), f.col("trait")))
            .select("studyId", "region", f.col("cs").alias("credibleSetIndex"))
        )

        processed_finngen_finemapping_df = processed_finngen_finemapping_df.join(
            finngen_finemapping_summaries,
            on=["studyId", "region", "credibleSetIndex"],
            how="inner",
        )

        toploci_df = (
            processed_finngen_finemapping_df.withColumn(
                "rn",
                f.row_number().over(
                    Window.partitionBy("studyId", "region", "credibleSetIndex").orderBy(
                        *[
                            f.col("pValueExponent").asc(),
                            f.col("pValueMantissa").asc(),
                        ]
                    )
                ),
            )
            .filter(f.col("rn") == 1)
            .select(
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
            )
        )

        processed_finngen_finemapping_df = (
            processed_finngen_finemapping_df.groupBy(
                "studyId", "region", "credibleSetIndex"
            )
            .agg(
                f.collect_list("variantId").alias("variantId"),
                f.collect_list("posteriorProbability").alias("posteriorProbability"),
                f.collect_list("lbf").alias("lbf"),
                f.collect_list("pValueMantissa").alias("pValueMantissa"),
                f.collect_list("pValueExponent").alias("pValueExponent"),
                f.collect_list("beta").alias("beta"),
                f.collect_list("standardError").alias("standardError"),
            )
            .select(
                "studyId",
                "region",
                "credibleSetIndex",
                f.array(
                    f.struct(
                        f.col("variantId"),
                        f.col("posteriorProbability"),
                        f.col("lbf"),
                        f.col("pValueMantissa"),
                        f.col("pValueExponent"),
                        f.col("beta"),
                        f.col("standardError"),
                    )
                ).alias("locus"),
            )
            .join(toploci_df, on=["studyId", "region", "credibleSetIndex"], how="inner")
        )

        return cls(
            _df=processed_finngen_finemapping_df,
            _schema=cls.get_schema(),
        )
