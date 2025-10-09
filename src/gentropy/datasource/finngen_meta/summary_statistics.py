"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.datasource.finngen_meta import MetaAnalysisDataSource


class FinnGenMetaSummaryStatistics:
    """FinnGen meta summary statistics ingestion and harmonisation."""

    @staticmethod
    def extract_study_phenotype_from_path(file_path: Column) -> Column:
        """Extract the study phenotype from finngen file path.

        Note:
            Assumes the file name format is some_path/to/<studyPhenotype>_meta_out.tsv.gz
        """
        return f.regexp_replace(
            f.element_at(f.split(file_path, "/"), -1), "_meta_out.tsv.gz", ""
        )

    # raw_schema represents the order of columns in the original summary statistics files
    raw_schema = t.StructType(
        [
            t.StructField("#CHR", t.StringType(), True),
            t.StructField("POS", t.LongType(), True),
            t.StructField("REF", t.StringType(), True),
            t.StructField("ALT", t.StringType(), True),
            t.StructField("SNP", t.StringType(), True),
            # fg_ (FinnGen)
            t.StructField("fg_beta", t.DoubleType(), True),
            t.StructField("fg_sebeta", t.DoubleType(), True),
            t.StructField("fg_pval", t.DoubleType(), True),
            t.StructField("fg_af_alt", t.DoubleType(), True),
            t.StructField("fg_af_alt_cases", t.DoubleType(), True),
            t.StructField("fg_af_alt_controls", t.DoubleType(), True),
            # MVP_EUR
            t.StructField("MVP_EUR_beta", t.DoubleType(), True),
            t.StructField("MVP_EUR_sebeta", t.DoubleType(), True),
            t.StructField("MVP_EUR_pval", t.DoubleType(), True),
            t.StructField("MVP_EUR_af_alt", t.DoubleType(), True),
            t.StructField("MVP_EUR_r2", t.DoubleType(), True),
            # MVP_AFR
            t.StructField("MVP_AFR_beta", t.DoubleType(), True),
            t.StructField("MVP_AFR_sebeta", t.DoubleType(), True),
            t.StructField("MVP_AFR_pval", t.DoubleType(), True),
            t.StructField("MVP_AFR_af_alt", t.DoubleType(), True),
            t.StructField("MVP_AFR_r2", t.DoubleType(), True),
            # MVP_HIS
            t.StructField("MVP_HIS_beta", t.DoubleType(), True),
            t.StructField("MVP_HIS_sebeta", t.DoubleType(), True),
            t.StructField("MVP_HIS_pval", t.DoubleType(), True),
            t.StructField("MVP_HIS_af_alt", t.DoubleType(), True),
            t.StructField("MVP_HIS_r2", t.DoubleType(), True),
            # UKBB
            t.StructField("ukbb_beta", t.DoubleType(), True),
            t.StructField("ukbb_sebeta", t.DoubleType(), True),
            t.StructField("ukbb_pval", t.DoubleType(), True),
            t.StructField("ukbb_af_alt", t.DoubleType(), True),
            # Meta
            t.StructField("all_meta_N", t.IntegerType(), True),
            t.StructField("all_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("all_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("all_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("all_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("all_inv_var_het_p", t.DoubleType(), True),
            # Leave-one-out: FinnGen
            t.StructField("leave_fg_N", t.IntegerType(), True),
            t.StructField("leave_fg_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("leave_fg_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("leave_fg_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("leave_fg_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("leave_fg_inv_var_meta_het_p", t.DoubleType(), True),
            # Leave-one-out: MVP_EUR
            t.StructField("leave_MVP_EUR_N", t.IntegerType(), True),
            t.StructField("leave_MVP_EUR_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("leave_MVP_EUR_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("leave_MVP_EUR_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("leave_MVP_EUR_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("leave_MVP_EUR_inv_var_meta_het_p", t.DoubleType(), True),
            # Leave-one-out: MVP_AFR
            t.StructField("leave_MVP_AFR_N", t.IntegerType(), True),
            t.StructField("leave_MVP_AFR_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("leave_MVP_AFR_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("leave_MVP_AFR_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("leave_MVP_AFR_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("leave_MVP_AFR_inv_var_meta_het_p", t.DoubleType(), True),
            # Leave-one-out: MVP_HIS
            t.StructField("leave_MVP_HIS_N", t.IntegerType(), True),
            t.StructField("leave_MVP_HIS_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("leave_MVP_HIS_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("leave_MVP_HIS_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("leave_MVP_HIS_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("leave_MVP_HIS_inv_var_meta_het_p", t.DoubleType(), True),
            # Leave-one-out: UKBB
            t.StructField("leave_ukbb_N", t.IntegerType(), True),
            t.StructField("leave_ukbb_inv_var_meta_beta", t.DoubleType(), True),
            t.StructField("leave_ukbb_inv_var_meta_sebeta", t.DoubleType(), True),
            t.StructField("leave_ukbb_inv_var_meta_p", t.DoubleType(), True),
            t.StructField("leave_ukbb_inv_var_meta_mlogp", t.DoubleType(), True),
            t.StructField("leave_ukbb_inv_var_meta_het_p", t.DoubleType(), True),
            # RSID
            t.StructField("rsid", t.StringType(), True),
        ]
    )

    @classmethod
    def bgzip_to_parquet(
        cls,
        session: Session,
        summary_statistics_glob: str,
        datasource: MetaAnalysisDataSource,
        raw_summary_statistics_output_path: str,
    ) -> None:
        """Convert gzipped summary statistics to Parquet format.

        Args:
            session (Session): Session object.
            summary_statistics_glob (str): Base path where summary statistics files are located.
            datasource (MetaAnalysisDataSource): Data source information.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
        """
        session.logger.info(
            f"Converting gzipped summary statistics from {summary_statistics_glob} to Parquet at {raw_summary_statistics_output_path}."
        )
        if not session.use_enhanced_bgzip_codec:
            session.logger.error(
                "The use_enhanced_bgzip_codec is set to False. This will lead to inefficient reading of block gzipped files."
            )
            raise KeyError(
                "Please set `session.spark.use_enhanced_bgzip_codec` to True in the Session configuration."
            )

        (
            session.spark.read.csv(
                summary_statistics_glob, schema=cls.raw_schema, sep="\t", header=True
            )
            .withColumn(
                "studyId",
                f.concat_ws(
                    "_",
                    f.lit(datasource.value),
                    f.lit(cls.extract_study_phenotype_from_path(f.input_file_name())),
                ),
            )
            .write.mode(session.write_mode)
            .partitionBy("studyId")
            .parquet(raw_summary_statistics_output_path)
        )

    @classmethod
    def harmonise(
        cls,
        session: Session,
        raw_summary_statistics_path: str,
    ) -> SummaryStatistics:
        """Load raw summary statistics from Parquet files.

        Args:
            session (Session): Session object.
            raw_summary_statistics_path (str): Path to the raw summary statistics Parquet files.
            study_index (StudyIndex): Study index object.
        """
        raw_sumstats = session.spark.read.parquet(raw_summary_statistics_path)
        session.logger.info("Harmonising summary statistics.")

        sumstats = raw_sumstats.select(
            f.col("studyId"),
            f.col("variantId"),
            f.col("chromosome"),
            f.col("position").cast(t.IntegerType()).alias("position"),
            f.col("beta").cast(t.DoubleType()).alias("beta"),
            f.col("sampleSize").cast(t.IntegerType()).alias("sampleSize"),
            *pvalue_from_neglogpval(f.col("negLogPval")),
            f.lit(None).cast(t.DoubleType()).alias("effectAlleleFrequencyFromSource"),
            f.col("standardError").cast(t.DoubleType()).alias("standardError"),
        )
        return SummaryStatistics(_df=sumstats)

    @staticmethod
    def maf(af: Column) -> Column:
        """Calculate minor allele frequency from allele frequency."""
        return (
            f.when(af.isNotNull() & (af <= 0.5), af)
            .when(af.isNotNull(), 1 - af)
            .otherwise(f.lit(None))
            .alias("minorAlleleFrequency")
        )

    @classmethod
    def allele_frequencies(cls, af_threshold: float = 1e-4) -> Column:
        return f.filter(
            f.array(
                f.struct(
                    f.col("MVP_EUR_af_alt").alias("alleleFrequency"),
                    cls.maf(f.col("MVP_EUR_af_alt")).alias("minAlleleFrequency"),
                    f.lit("MVP_EUR").alias("cohort"),
                    f.when(
                        f.col("MVP_EUR_af_alt").isNull()
                        | (cls.maf(f.col("MVP_EUR_af_alt")) > af_threshold),
                        f.lit(True),
                    )
                    .otherwise(f.lit(False))
                    .alias("filter"),
                ),
                f.struct(
                    f.col("MVP_AFR_af_alt").alias("alleleFrequency"),
                    cls.maf(f.col("MVP_AFR_af_alt")).alias("minAlleleFrequency"),
                    f.lit("MVP_AFR").alias("cohort"),
                    f.when(
                        f.col("MVP_AFR_af_alt").isNull()
                        | (cls.maf(f.col("MVP_AFR_af_alt")) > af_threshold),
                        f.lit(True),
                    )
                    .otherwise(f.lit(False))
                    .alias("filter"),
                ),
                f.struct(
                    f.col("MVP_HIS_af_alt").alias("alleleFrequency"),
                    cls.maf(f.col("MVP_HIS_af_alt")).alias("minAlleleFrequency"),
                    f.lit("MVP_AMR").alias("cohort"),
                    f.when(
                        f.col("MVP_HIS_af_alt").isNull()
                        | (cls.maf(f.col("MVP_HIS_af_alt")) > af_threshold),
                        f.lit(True),
                    )
                    .otherwise(f.lit(False))
                    .alias("filter"),
                ),
                f.struct(
                    f.col("fg_af_alt").alias("alleleFrequency"),
                    cls.maf(f.col("fg_af_alt")).alias("minAlleleFrequency"),
                    f.lit("FinnGen").alias("cohort"),
                    f.when(
                        f.col("fg_af_alt").isNull()
                        | (cls.maf(f.col("fg_af_alt")) > af_threshold),
                        f.lit(True),
                    )
                    .otherwise(f.lit(False))
                    .alias("filter"),
                ),
                f.struct(
                    f.col("ukbb_af_alt").alias("alleleFrequency"),
                    cls.maf(f.col("ukbb_af_alt")).alias("minAlleleFrequency"),
                    f.lit("UKBB").alias("cohort"),
                    f.when(
                        f.col("ukbb_af_alt").isNull()
                        | (cls.maf(f.col("ukbb_af_alt")) > af_threshold),
                        f.lit(True),
                    )
                    .otherwise(f.lit(False))
                    .alias("filter"),
                ),
                lambda x: x["alleleFrequency"].isNotNull(),
            )
        ).alias("alleleFrequencies")

    @classmethod
    def imputation_score(cls, imputation_threshold: float = 0.8) -> Column:
        return f.array(
            f.struct(
                f.col("MVP_EUR_r2").alias("r2"),
                f.lit("MVP_EUR").alias("cohort"),
                f.when(
                    f.col("MVP_EUR_r2").isNull()
                    | (f.col("MVP_EUR_r2") >= imputation_threshold),
                    f.lit(True),
                )
                .otherwise(f.lit(False))
                .alias("filter"),
            ),
            f.struct(
                f.col("MVP_AFR_r2").alias("r2"),
                f.lit("MVP_AFR").alias("cohort"),
                f.when(
                    f.col("MVP_AFR_r2").isNull()
                    | (f.col("MVP_AFR_r2") >= imputation_threshold),
                    f.lit(True),
                )
                .otherwise(f.lit(False))
                .alias("filter"),
            ),
            f.struct(
                f.col("MVP_HIS_r2").alias("r2"),
                f.lit("MVP_AMR").alias("cohort"),
                f.when(
                    f.col("MVP_HIS_r2").isNull()
                    | (f.col("MVP_HIS_r2") >= imputation_threshold),
                    f.lit(True),
                )
                .otherwise(f.lit(False))
                .alias("filter"),
            ),
        ).alias("imputationScore")

    @classmethod
    def cohorts(cls) -> Column:
        return f.transform(
            f.array(
                f.filter(
                    f.struct(
                        f.when(f.col("MVP_EUR_af_alt").isNotNull(), f.lit(True))
                        .otherwise(f.lit(False))
                        .alias("inCohort"),
                        f.lit("MVP_EUR").alias("cohort"),
                        f.lit("MVP").alias("biobank"),
                    ),
                    f.struct(
                        f.when(f.col("MVP_AFR_af_alt").isNotNull(), f.lit(True))
                        .otherwise(f.lit(False))
                        .alias("inCohort"),
                        f.lit("MVP_AFR").alias("cohort"),
                        f.lit("MVP").alias("biobank"),
                    ),
                    f.struct(
                        f.when(f.col("MVP_HIS_af_alt").isNotNull(), f.lit(True))
                        .otherwise(f.lit(False))
                        .alias("inCohort"),
                        f.lit("MVP_AMR").alias("cohort"),
                        f.lit("MVP").alias("biobank"),
                    ),
                    f.struct(
                        f.when(f.col("fg_af_alt").isNotNull(), f.lit(True))
                        .otherwise(f.lit(False))
                        .alias("inCohort"),
                        f.lit("FinnGen").alias("cohort"),
                        f.lit("FinnGen").alias("biobank"),
                    ),
                    f.struct(
                        f.when(f.col("ukbb_af_alt").isNotNull(), f.lit(True))
                        .otherwise(f.lit(False))
                        .alias("inCohort"),
                        f.lit("UKBB").alias("cohort"),
                        f.lit("UKBB").alias("biobank"),
                    ),
                    lambda x: x["inCohort"],
                ),
                lambda x: f.struct(x["biobank"], x["cohort"]),
            )
        ).alias("cohorts")

    @classmethod
    def n_biobanks(cls, cohorts: Column) -> Column:
        return f.reduce(
            f.array_distinct(
                f.transform(
                    f.col("metaCohorts"),
                    lambda x: f.struct(
                        x["biobank"],
                        x["inCohort"].cast(t.IntegerType()).alias("inCohort"),
                    ),
                )
            ),
            f.lit(0),
            lambda acc, x: acc + x["inCohort"],
        ).alias("nBiobanks")

    @classmethod
    def is_meta_analyzed_variant(cls, n_biobanks: Column) -> Column:
        return (
            f.when(n_biobanks > 1, f.lit(True))
            .otherwise(f.lit(False))
            .alias("isMetaAnalyzedVariant")
        )

    @classmethod
    def has_low_min_allele_frequency(cls, allele_frequencies: Column) -> Column:
        return (f.size(f.filter(allele_frequencies, lambda x: ~x["filter"])) > 0).alias(
            "hasLowMinAlleleFrequency"
        )

    @classmethod
    def has_low_imputation_score(cls, imputation_r2: Column) -> Column:
        return (f.size(f.filter(imputation_r2, lambda x: ~x["filter"])) > 0).alias(
            "hasLowImputationScore"
        )

    @classmethod
    def chromosome(cls) -> Column:
        return (
            f.when(f.col("#CHR").cast(t.StringType()) == f.lit("23"), f.lit("X"))
            .otherwise(f.col("#CHR").cast(t.StringType()))
            .alias("chromosome")
        )

    @classmethod
    def variant_id(cls) -> Column:
        return f.concat_ws(
            "_",
            f.col("chromosome"),
            f.col("position"),
            f.col("reference"),
            f.col("alternate"),
        ).alias("variantId")

    def _harmonise(self, df: DataFrame) -> DataFrame:
        return (
            df.filter(f.col("all_inv_var_meta_mlogp").isNotNull())
            .filter(f.col("all_inv_var_meta_beta").isNotNull())
            .filter(f.col("all_inv_var_meta_sebeta").isNotNull())
            # Filter by the meta analyzed biobank count > 1
            .withColumn("cohorts", self.cohorts())
            .withColumn("metaCohorts", self.meta_cohorts())
            .withColumn("nBiobanks", self.n_biobanks())
            .withColumn("isMetaAnalyzedVariant", self.is_meta_analyzed_variant())
            .filter(f.col("isMetaAnalyzedVariant"))
            # .drop("isMetaAnalyzedVariant", "cohorts", "metaCohorts", "nBiobanks")
            # Filter by imputation score < 0.8
            .withColumn("imputationR2", self.imputation_score())
            .withColumn("hasLowImputationScore", self.has_low_imputation_score())
            .filter(~f.col("hasLowImputationScore"))
            # .drop("hasLowImputationScore", "imputationR2")
            # Filter by the allele frequency < 0.0001
            .withColumn("alleleFrequencies", self.allele_frequencies())
            .withColumn("hasLowMinAlleleFrequency", self.has_low_min_allele_frequency())
            .filter(~f.col("hasLowMinAlleleFrequency"))
            # .drop("hasLowMinAlleleFrequency", "alleleFrequencies", "filteredAlleleFrequencies")
            .select(
                f.col("studyId"),
                self.chromosome().alias("chromosome"),
                f.col("POS").cast(t.IntegerType()).alias("position"),
                f.col("REF").cast(t.StringType()).alias("reference"),
                f.col("ALT").cast(t.StringType()).alias("alternate"),
                f.col("all_inv_var_meta_beta").alias("beta"),
                f.col("all_inv_var_meta_sebeta").alias("standardError"),
                *pvalue_from_neglogpval(f.col("all_inv_var_meta_mlogp")),
                f.col("isMetaAnalyzedVariant"),
                f.col("nBiobanks"),
                f.col("metaCohorts"),
                f.col("filteredAlleleFrequencies"),
                f.col("hasLowMinAlleleFrequency"),
                f.col("hasLowImputationScore"),
                f.col("imputationR2"),
            )
            .select(
                f.col("studyId"),
                self.variant_id().alias("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.col("pValueMantissa"),
                f.col("pValueExponent"),
                f.col("standardError"),
                f.col("isMetaAnalyzedVariant"),
                f.col("nBiobanks"),
                f.col("metaCohorts"),
                f.col("filteredAlleleFrequencies"),
                f.col("hasLowMinAlleleFrequency"),
                f.col("hasLowImputationScore"),
                f.col("imputationR2"),
            )
        )
