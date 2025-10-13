"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import normalize_chromosome
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen_meta import FinnGenMetaManifest, MetaAnalysisDataSource


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
    def from_source(
        cls,
        raw_summary_statistics: DataFrame,
        finngen_manifest: FinnGenMetaManifest,
        variant_annotations: VariantDirection,
        imputation_score_threshold: float = 0.8,
        min_allele_count_threshold: int = 20,
    ) -> SummaryStatistics:
        si_slice = finngen_manifest.df.select(
            f.col("studyId"),
            f.col("nCasesPerCohort"),
            f.col("nSamples"),
        )
        vd_slice = variant_annotations.df.select(
            f.col("originalVariantId"),
            f.col("variantId"),
            f.col("direction"),
            f.col("isPalindromic"),
        ).repartition("chromosome")
        sumstats = (
            raw_summary_statistics
            # Filter out rows with missing statistics
            .filter(f.col("all_inv_var_meta_mlogp").isNotNull())
            .filter(f.col("all_inv_var_meta_beta").isNotNull())
            .filter(f.col("all_inv_var_meta_sebeta").isNotNull())
            # Filter out variants that are not meta analyzed (nBiobanks < 1)
            .withColumn("cohorts", cls.cohorts())
            .withColumn(
                "isMetaAnalyzedVariant",
                cls.is_meta_analyzed_variant(f.col("cohorts")),
            )
            .filter(f.col("isMetaAnalyzedVariant"))
            .drop("isMetaAnalyzedVariant", "cohorts")
            # Filter out variants from MVP cohorts that have low imputation score
            .withColumn(
                "imputationScore",
                cls.imputation_score(imputation_score_threshold),
            )
            .withColumn(
                "hasLowImputationScore",
                cls.has_low_imputation_score(f.col("imputationScore")),
            )
            .filter(~f.col("hasLowImputationScore"))
            .drop("hasLowImputationScore", "imputationScore")
            # Annotate with StudyIndex nCases to obtain the cases Minor Allele Count
            .join(si_slice, on="studyId", how="left")
            # Calculate Minor Allele Count (MAC)
            .withColumn("cohortAlleleFrequency", cls.allele_frequencies())
            .withColumn(
                "cohortMinAlleleFrequency",
                cls.min_allele_frequency(f.col("cohortAlleleFrequency")),
            )
            .withColumn(
                "cohortMinAlleleCount",
                cls.min_allele_count(f.col("cohortAlleleCount"), f.col("nCases")),
            )
            .withColumn(
                "hasLowMinAlleleCount",
                cls.has_low_min_allele_count(
                    f.col("cohortMinAlleleCount"),
                    min_allele_count_threshold,
                ),
            )
            .filter(~f.col("hasLowMinAlleleCount"))
            .drop(
                "hasLowMinAlleleCount",
                "cohortAlleleFrequency",
                "cohortMinAlleleFrequency",
                "cohortMinAlleleCount",
                "nCases",
            )
            # Select and rename columns
            .select(
                f.col("studyId"),
                f.col("#CHR").cast(t.StringType()).alias("chromosome"),
                f.col("POS").cast(t.IntegerType()).alias("position"),
                f.col("REF").alias("referenceAllele"),
                f.col("ALT").alias("alternateAllele"),
                *pvalue_from_neglogpval(f.col("all_inv_var_meta_mlogp")),
                f.col("all_inv_var_meta_beta").cast(t.DoubleType()).alias("beta"),
                f.col("nSamples").alias("sampleSize"),
                f.col("all_inv_var_meta_sebeta")
                .cast(t.DoubleType())
                .alias("standardError"),
            )
            .withColumn("chromosome", normalize_chromosome(f.col("chromosome")))
            .withColumn(
                "variantId",
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("reference"),
                    f.col("alternate"),
                ).alias("variantId"),
            )
            .select(
                f.col("studyId"),
                f.col("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.col("sampleSize"),
                f.col("pValueMantissa"),
                f.col("pValueExponent"),
                f.lit(None)
                .cast(t.FloatType())
                .alias("effectAlleleFrequencyFromSource"),
                f.col("standardError"),
            )
            # Join with variant direction and align allele orientation, beta sign, and allele frequency
            # Drop variants that are not present in variant index (any direction)
            .repartition("chromosome")
            .join(
                vd_slice,
                on=["chromosome", "variantId"],
                how="inner",
            )
            # Remove palindromic variants
            .filter(~f.col("isPalindromic"))
            .drop("isPalindromic")
            # Keep the originalVariantId as variantId - this is aligned with the variant index
            .drop("variantId")
            .withColumnRenamed("originalVariantId", "variantId")
            # Flip the beta sign
            .withColumn("beta", f.col("beta") * f.col("direction"))
            # Nothing to be done on AF side, as we do not report it, since we have a mix of ancestries
            .select(
                f.col("studyId"),
                f.col("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.col("sampleSize"),
                f.col("pValueMantissa"),
                f.col("pValueExponent"),
                f.col("effectAlleleFrequencyFromSource"),
                f.col("standardError"),
            )
        )

        return SummaryStatistics(sumstats).sanity_filter()

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
                    f.lit("MVP_EUR").alias("cohort"),
                    f.col("MVP_EUR_af_alt").alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    f.col("MVP_AFR_af_alt").alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    f.col("MVP_HIS_af_alt").alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("FinnGen").alias("cohort"),
                    f.col("fg_af_alt").alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("UKBB").alias("cohort"),
                    f.col("ukbb_af_alt").alias("alleleFrequency"),
                ),
            ),
            lambda x: x.getField("alleleFrequency").isNotNull(),
        ).alias("alleleFrequencies")

    @classmethod
    def min_allele_frequency(cls, allele_freq: Column) -> Column:
        return f.transform(
            allele_freq,
            lambda x: f.struct(
                x.getField("cohort").alias("cohort"),
                cls.maf(x.getField("alleleFrequency")).alias("minAlleleFrequency"),
            ),
        )

    @classmethod
    def min_allele_count(
        cls, cohort_min_allele_frequency: Column, n_cases_per_cohort: Column
    ) -> Column:
        # Take two arrays of structs and zip them together, then calculate the min allele count

        return f.transform(
            f.filter(
                cohort_min_allele_frequency,
                lambda left: f.exists(
                    n_cases_per_cohort,
                    lambda right: right.getField("cohort") == left.getField("cohort"),
                ),
            ),
            lambda left: f.struct(
                left.getField("cohort").alias("cohort"),
                (
                    left.getField("minAlleleFrequency")
                    * f.lit(2)
                    * f.filter(
                        n_cases_per_cohort,
                        lambda right: right.getField("cohort")
                        == left.getField("cohort").getItem(0).getField("nCases"),
                    ).alias("minAlleleCount")
                ),
            ),
        )

    @classmethod
    def has_low_min_allele_count(
        cls, min_allele_count: Column, min_allele_count_threshold: int = 20
    ) -> Column:
        return (
            f.size(
                f.filter(
                    min_allele_count,
                    lambda x: x.getField("minAlleleCount")
                    < f.lit(min_allele_count_threshold),
                )
            )
            > 0
        ).alias("hasLowMinAlleleCount")

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
    def has_low_imputation_score(cls, imputation_r2: Column) -> Column:
        return (f.size(f.filter(imputation_r2, lambda x: ~x["filter"])) > 0).alias(
            "hasLowImputationScore"
        )

    @classmethod
    def cohorts(cls) -> Column:
        """Cohorts involved in the meta-analysis."""
        return f.transform(
            f.filter(
                f.array(
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
                ),
                lambda x: x["inCohort"],
            ),
            lambda x: f.struct(x["biobank"], x["cohort"]),
        ).alias("cohorts")

    @classmethod
    def is_meta_analyzed_variant(cls, cohorts: Column) -> Column:
        """Check if the variant is meta-analyzed (present in more than one biobank).

        Args:
            cohorts (Column): Array of structs with fields 'biobank'.

        Returns:
            Column: Boolean column indicating if the variant is meta-analyzed.

        Examples:

            >>> data = [([("FinnGen", "FinnGen"), ("MVP", "MVP_EUR"), ("MVP", "MVP_AMR")],), ([("MVP", "MVP_AMR"), ("MVP", "MVP_EUR")],),([("UKBB", "UKBB")],)]
            >>> schema = "cohorts ARRAY<STRUCT<biobank: STRING, cohort: STRING>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +--------------------------------------------------+
            |cohorts                                           |
            +--------------------------------------------------+
            |[{FinnGen, FinnGen}, {MVP, MVP_EUR}, {MVP, MVP_AMR}]|
            |[{MVP, MVP_AMR}, {MVP, MVP_EUR}]                      |
            |[{UKBB, UKBB}]                                       |
            >>> df.withColumn("isMetaAnalyzedVariant", FinnGenMetaSummaryStatistics.is_meta_analyzed_variant(f.col("cohorts"))).show(truncate=False)
            +-------+-----------------------+
            |cohorts|isMetaAnalyzedVariant  |
            +-------+-----------------------+
            |[{FinnGen, FinnGen}, {MVP, MVP_EUR}, {MVP, MVP_AMR}]|true                   |
            |[{MVP, MVP_AMR}, {MVP, MVP_EUR}]|false                  |
            |[{UKBB, UKBB}]                                       |false                  |
            +-------+-----------------------+



        """
        n_biobanks = f.size(
            f.array_distinct(f.transform(cohorts, lambda x: x.getField("biobank"))),
        )
        return (n_biobanks > 1).alias("isMetaAnalyzedVariant")
