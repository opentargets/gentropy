"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from concurrent.futures import ThreadPoolExecutor
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import mac, maf, normalize_chromosome
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


        Args:
            file_path (Column): Column containing the file path as a string.

        Returns:
            Column: Extracted study phenotype as a string.

        Examples:
            >>> data = [("/path/to/AB1_meta_out.tsv.gz",), ("/another/path/CD2_meta_out.tsv.gz",)]
            >>> schema = "filePath STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df =df.withColumn("studyPhenotype", FinnGenMetaSummaryStatistics.extract_study_phenotype_from_path(f.col("filePath")))
            >>> df.show(truncate=False)
            +---------------------------------+--------------+
            |filePath                         |studyPhenotype|
            +---------------------------------+--------------+
            |/path/to/AB1_meta_out.tsv.gz     |AB1           |
            |/another/path/CD2_meta_out.tsv.gz|CD2           |
            +---------------------------------+--------------+
            <BLANKLINE>

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
            # FinnGen
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
        summary_statistics_list: list[str],
        datasource: MetaAnalysisDataSource,
        raw_summary_statistics_output_path: str,
    ) -> None:
        """Convert gzipped summary statistics to Parquet format.

        Args:
            session (Session): Session object.
            summary_statistics_list (list[str]): List of paths where summary statistics files are located.
            datasource (MetaAnalysisDataSource): Data source information.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
        """
        if not session.use_enhanced_bgzip_codec:
            session.logger.error(
                "The use_enhanced_bgzip_codec is set to False. This will lead to inefficient reading of block gzipped files."
            )
            raise KeyError(
                "Please set `session.spark.use_enhanced_bgzip_codec` to True in the Session configuration."
            )

        def process_one(
            input_path: str, session: Session, output_path: str
        ) -> DataFrame:
            df = session.spark.read.csv(
                input_path,
                header=True,
                inferSchema=True,
                sep="\t",
                enforceSchema=False,
            )
            # Apply schema enforcement by selecting columns in the expected order with proper types
            for c in cls.raw_schema.names:
                if c not in df.columns:
                    df = df.withColumn(c, f.lit(None).cast(cls.raw_schema[c].dataType))

            # Replace all NA with nulls and cast to the expected type
            for field in cls.raw_schema.fields:
                df = df.withColumn(
                    field.name,
                    f.when(f.col(field.name) == "NA", f.lit(None))
                    .otherwise(f.col(field.name))
                    .cast(field.dataType),
                )
            df = df.withColumn(
                "studyId",
                f.concat_ws(
                    "_",
                    f.lit(datasource.value),
                    f.lit(cls.extract_study_phenotype_from_path(f.input_file_name())),
                ),
            )
            df.write.mode("append").partitionBy("studyId").parquet(output_path)
            return df

        session.logger.info(
            f"Converting gzipped summary statistics from {summary_statistics_list} to Parquet at {raw_summary_statistics_output_path}."
        )
        with ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(
                lambda path: process_one(
                    path,
                    session=session,
                    output_path=raw_summary_statistics_output_path,
                ),
                summary_statistics_list,
            )

    @classmethod
    def from_source(
        cls,
        raw_summary_statistics: DataFrame,
        finngen_manifest: FinnGenMetaManifest,
        variant_annotations: VariantDirection,
        perform_meta_analysis_filter: bool = True,
        imputation_score_threshold: float = 0.8,
        perform_imputation_score_filter: bool = True,
        min_allele_count_threshold: int = 20,
        perform_min_allele_count_filter: bool = False,
        min_allele_frequency_threshold: float = 1e-4,
        perform_min_allele_frequency_filter: bool = True,
        filter_out_ambiguous_variants: bool = False,
    ) -> SummaryStatistics:
        """Build the summary statistics dataset from raw summary statistics.

        The logic behind the harmonisation has following steps:
        (1) Filter out rows with missing statistics (p-value, beta, standard error)
        (2) Filter out variants that are not meta analyzed (nBiobanks < 1)
        (3) Filter out variants from MVP cohorts that have low imputation score < 0.8
        (4) Filter out variants that have low minor allele count < 20 in any cohort
        (6) Align allele direction, beta sign based on the Gnomad variant direction dataset.
        (7) Perform the summary statistics

        To calculate the MAC we need to bring the number of cases per cohort from the StudyIndex.

        Args:
            raw_summary_statistics (DataFrame): Raw summary statistics dataframe.
            finngen_manifest (FinnGenMetaManifest): FinnGen meta analysis manifest.
            variant_annotations (VariantDirection): Variant direction dataset.
            perform_meta_analysis_filter (bool): Whether to remove variants found in just 1 biobank. Default is True.
            imputation_score_threshold (float): Imputation score threshold for MVP cohorts. Default is 0.8.
            perform_imputation_score_filter (bool): Whether to perform the imputation score filter. Default is True.
            min_allele_count_threshold (int): Minimum allele count threshold. Default is 20.
            perform_min_allele_count_filter (bool): Whether to perform the minimum allele count filter. Default is False.
            min_allele_frequency_threshold (float): Minimum allele frequency threshold. Default is 1e-4.
            perform_min_allele_frequency_filter (bool): Whether to perform the minimum allele frequency filter. Default is True.

        Returns:
            SummaryStatistics: Processed summary statistics dataset.
        """
        si_slice = finngen_manifest.df.select(
            f.col("studyId"),
            f.col("nCasesPerCohort"),
            f.col("nSamples"),
        )
        vd_slice = variant_annotations.df.select(
            f.col("chromosome"),
            f.col("originalVariantId"),
            f.col("variantId"),
            f.col("direction"),
            f.col("isStrandAmbiguous"),
        ).repartition("chromosome")

        # Filter out rows with missing statistics
        sumstats = (
            raw_summary_statistics.filter(f.col("all_inv_var_meta_mlogp").isNotNull())
            .filter(f.col("all_inv_var_meta_beta").isNotNull())
            .filter(f.col("all_inv_var_meta_sebeta").isNotNull())
        )

        # Filter out variants that are not meta analyzed (nBiobanks < 1)
        if perform_meta_analysis_filter:
            sumstats = (
                sumstats.withColumn("cohorts", cls.cohorts())
                .withColumn(
                    "isMetaAnalyzedVariant",
                    cls.is_meta_analyzed_variant(f.col("cohorts")),
                )
                .filter(f.col("isMetaAnalyzedVariant"))
                .drop("isMetaAnalyzedVariant", "cohorts")
            )
        # Filter out variants with low INFO score
        if perform_imputation_score_filter:
            sumstats = (
                sumstats.withColumn(
                    "hasLowImputationScore",
                    cls.has_low_imputation_score(imputation_score_threshold),
                )
                .filter(~f.col("hasLowImputationScore"))
                .drop("hasLowImputationScore", "imputationScore")
            )

        # Annotate with StudyIndex nCases to obtain the cases Minor Allele Count
        sumstats = sumstats.join(si_slice, on="studyId", how="left")

        if perform_min_allele_count_filter or perform_min_allele_frequency_filter:
            sumstats = (
                sumstats
                # Collapse allele frequencies into an array of structs
                .withColumn("cohortAlleleFrequency", cls.allele_frequencies())
                # Calculate the MAF per cohort
                .withColumn(
                    "cohortMinAlleleFrequency",
                    cls.min_allele_frequency(f.col("cohortAlleleFrequency")),
                )
            )
        if perform_min_allele_count_filter:
            sumstats = (
                sumstats
                # Make sure to only keep cohorts that have nCases > 0
                .withColumn(
                    "nCasesPerCohort",
                    f.filter(
                        f.col("nCasesPerCohort"),
                        lambda x: x.getField("nCases").isNotNull()
                        & (x.getField("nCases") > 0),
                    ),
                )
                .withColumn(
                    "cohortMinAlleleCount",
                    cls.min_allele_count(
                        f.col("cohortMinAlleleFrequency"),
                        f.col("nCasesPerCohort"),
                    ),
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
                    "cohortMinAlleleCount",
                    "nCasesPerCohort",
                )
            )
        if perform_min_allele_frequency_filter:
            sumstats = (
                sumstats.withColumn(
                    "hasLowMinAlleleFrequency",
                    cls.has_low_min_allele_frequency(
                        f.col("cohortMinAlleleFrequency"),
                        min_allele_frequency_threshold,
                    ),
                )
                .filter(~f.col("hasLowMinAlleleFrequency"))
                .drop("hasLowMinAlleleFrequency")
            )

        sumstats = (
            sumstats.select(
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
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
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
                how="left",
            )
        )
        # Remove strand ambiguous variants, if not found in gnomAD, we keep the variant
        if filter_out_ambiguous_variants:
            sumstats = sumstats.filter(
                ~f.coalesce(f.col("isStrandAmbiguous"), f.lit(False))
            )
        #
        # Keep the originalVariantId as variantId - this is aligned with the variant index
        # if not found in gnomAD, keep the variantId as is
        sumstats = (
            sumstats.withColumn(
                "variantId", f.coalesce(f.col("originalVariantId"), f.col("variantId"))
            )
            # Flip the beta sign, make sure that if the variant was not found in gnomAD, we keep the beta as is
            .withColumn(
                "beta", f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))
            )
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

    @classmethod
    def has_low_min_allele_frequency(
        cls, maf: Column, threshold: float = 1e-4
    ) -> Column:
        """Find if variant has a low minor allele frequency in any cohort.

        Args:
            maf (Column): Column containing array of structs with `cohort` and `minAlleleFrequency` fields.
            threshold (float): Threshold below which the minor allele frequency is considered low. Default is 1e-4.

        Returns:
            Column: Boolean column indicating if any cohort has a low minor allele frequency.

        Examples:
            >>> maf = {"v1": [{"cohort": "A", "minAlleleFrequency": 0.0001}, {"cohort": "B", "minAlleleFrequency": 0.0002}],
            ...         "v2": [{"cohort": "A", "minAlleleFrequency": None}, {"cohort": "D", "minAlleleFrequency": 0.15}],
            ...         "v3": [{"cohort": "A", "minAlleleFrequency": 0.00001}, {"cohort": "B", "minAlleleFrequency": 0.2}],}
            >>> data = [("v1", maf["v1"]),
            ...         ("v2", maf["v2"]),
            ...         ("v3", maf["v3"]),]
            >>> schema = "variantId STRING, cohortMinAlleleFrequency ARRAY<STRUCT<cohort: STRING, minAlleleFrequency: DOUBLE>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+--------------------------+
            |variantId|cohortMinAlleleFrequency  |
            +---------+--------------------------+
            |v1       |[{A, 1.0E-4}, {B, 2.0E-4}]|
            |v2       |[{A, NULL}, {D, 0.15}]    |
            |v3       |[{A, 1.0E-5}, {B, 0.2}]   |
            +---------+--------------------------+
            <BLANKLINE>

            >>> df = df.withColumn("hasMinAlleleFrequency", FinnGenMetaSummaryStatistics.has_low_min_allele_frequency(f.col("cohortMinAlleleFrequency")))
            >>> df.select("variantId", "hasMinAlleleFrequency").show(truncate=False)
            +---------+---------------------+
            |variantId|hasMinAlleleFrequency|
            +---------+---------------------+
            |v1       |false                |
            |v2       |false                |
            |v3       |true                 |
            +---------+---------------------+
            <BLANKLINE>
        """
        non_empty_maf = f.filter(
            maf, lambda x: x.getField("minAlleleFrequency").isNotNull()
        )

        n_cohorts_with_maf_below_threshold = f.size(
            f.filter(
                non_empty_maf,
                lambda x: x.getField("minAlleleFrequency")
                < f.lit(threshold).cast(t.DecimalType(11, 10)),
            )
        )
        return n_cohorts_with_maf_below_threshold > 0

    @classmethod
    def allele_frequencies(cls, scale: int = 10) -> Column:
        """Extract the allele frequencies per cohort.

        Args:
            scale (int): Scale for the decimal type conversion. Default is 10.

        Returns:
            Column: Column containing array of structs with `cohort` and `alleleFrequency` fields.

        Examples:
            >>> data = [("v1", 0.1, 0.2, None, 0.3, 0.4),
            ...       ("v2", 0.000000001, 0.999999999, None, None, None),]
            >>> schema = "variantId STRING, MVP_EUR_af_alt DOUBLE, MVP_AFR_af_alt DOUBLE, MVP_HIS_af_alt DOUBLE, fg_af_alt DOUBLE, ukbb_af_alt DOUBLE"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+--------------+--------------+--------------+---------+-----------+
            |variantId|MVP_EUR_af_alt|MVP_AFR_af_alt|MVP_HIS_af_alt|fg_af_alt|ukbb_af_alt|
            +---------+--------------+--------------+--------------+---------+-----------+
            |v1       |0.1           |0.2           |NULL          |0.3      |0.4        |
            |v2       |1.0E-9        |0.999999999   |NULL          |NULL     |NULL       |
            +---------+--------------+--------------+--------------+---------+-----------+
            <BLANKLINE>

            >>> df = df.withColumn("alleleFrequencies", FinnGenMetaSummaryStatistics.allele_frequencies())
            >>> df.select("alleleFrequencies").show(truncate=False)
            +-------------------------------------------------------------------------------------------------+
            |alleleFrequencies                                                                                |
            +-------------------------------------------------------------------------------------------------+
            |[{MVP_EUR, 0.1000000000}, {MVP_AFR, 0.2000000000}, {FinnGen, 0.3000000000}, {UKBB, 0.4000000000}]|
            |[{MVP_EUR, 0.0000000010}, {MVP_AFR, 0.9999999990}]                                               |
            +-------------------------------------------------------------------------------------------------+
            <BLANKLINE>

        """
        precision = scale + 1  # to ensure we can represent values like 1.0000
        return f.filter(
            f.array(
                f.struct(
                    f.lit("MVP_EUR").alias("cohort"),
                    f.col("MVP_EUR_af_alt")
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    f.col("MVP_AFR_af_alt")
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    # Note: HIS in sumstats is AMR in study index
                    f.col("MVP_HIS_af_alt")
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("FinnGen").alias("cohort"),
                    f.col("fg_af_alt")
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("UKBB").alias("cohort"),
                    f.col("ukbb_af_alt")
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
            ),
            lambda x: x.getField("alleleFrequency").isNotNull(),
        ).alias("alleleFrequencies")

    @classmethod
    def min_allele_frequency(cls, allele_freq: Column) -> Column:
        """Minor Allele Frequency (MAF) per cohort.

        Note:
            The resulting value is of DecimalType(11, 10) to ensure precision for low frequency variants.

        Args:
            allele_freq (Column): Column containing array of structs with `cohort` and `alleleFrequency` fields.

        Returns:
            Column: Column containing array of structs with `cohort` and `minAlleleFrequency` fields.

        Examples:
            >>> data = [("v1", [{"cohort": "A", "alleleFrequency": 0.1}, {"cohort": "B", "alleleFrequency": 0.7}]),]
            >>> schema = "variantId STRING, alleleFrequencies ARRAY<STRUCT<cohort: STRING, alleleFrequency: DOUBLE>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+--------------------+
            |variantId|alleleFrequencies   |
            +---------+--------------------+
            |v1       |[{A, 0.1}, {B, 0.7}]|
            +---------+--------------------+
            <BLANKLINE>

            >>> df = df.withColumn("cohortMinAlleleFrequency", FinnGenMetaSummaryStatistics.min_allele_frequency(f.col("alleleFrequencies")))
            >>> df.show(truncate=False)
            +---------+--------------------+--------------------------------------+
            |variantId|alleleFrequencies   |cohortMinAlleleFrequency              |
            +---------+--------------------+--------------------------------------+
            |v1       |[{A, 0.1}, {B, 0.7}]|[{A, 0.1000000000}, {B, 0.3000000000}]|
            +---------+--------------------+--------------------------------------+
            <BLANKLINE>

        """
        return f.transform(
            allele_freq,
            lambda x: f.struct(
                x.getField("cohort").alias("cohort"),
                maf(x.getField("alleleFrequency")).alias("minAlleleFrequency"),
            ),
        )

    @classmethod
    def min_allele_count(
        cls, cohort_min_allele_frequency: Column, n_cases_per_cohort: Column
    ) -> Column:
        """Minor Allele Count (MAC) per cohort.

        Note:
            If a cohort either do not have the maf or nCases, it will be dropped from the resulting MAC array.

        Args:
            cohort_min_allele_frequency (Column): Column containing array of structs with `cohort` and `minAlleleFrequency` fields.
            n_cases_per_cohort (Column): Column containing array of structs with `cohort` and `nCases` fields.

        Returns:
            Column: Column containing array of structs with `cohort` and `minAlleleCount` fields.

        Examples:
            >>> maf = {"v1": [{"cohort": "A", "minAlleleFrequency": 0.1}, {"cohort": "B", "minAlleleFrequency": 0.2}],
            ...         "v2": [{"cohort": "A", "minAlleleFrequency": 0.05}, {"cohort": "D", "minAlleleFrequency": 0.15}],
            ...         "v3": [{"cohort": "A", "minAlleleFrequency": 0.01}, {"cohort": "B", "minAlleleFrequency": 0.02}],}
            >>> n_cases = {"v1": [{"cohort": "A", "nCases": 100}, {"cohort": "B", "nCases": 200}],
            ...            "v2": [{"cohort": "A", "nCases": 150}, {"cohort": "C", "nCases": 250}],
            ...            "v3": [{"cohort": "C", "nCases": 50}, {"cohort": "D", "nCases": 80}],}
            >>> data = [("v1", maf["v1"], n_cases["v1"]),
            ...         ("v2", maf["v2"], n_cases["v2"]),
            ...         ("v3", maf["v3"], n_cases["v3"]),]
            >>> schema = "variantId STRING, cohortMinAlleleFrequency ARRAY<STRUCT<cohort: STRING, minAlleleFrequency: DOUBLE>>, nCasesPerCohort ARRAY<STRUCT<cohort: STRING, nCases: INT>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+------------------------+--------------------+
            |variantId|cohortMinAlleleFrequency|nCasesPerCohort     |
            +---------+------------------------+--------------------+
            |v1       |[{A, 0.1}, {B, 0.2}]    |[{A, 100}, {B, 200}]|
            |v2       |[{A, 0.05}, {D, 0.15}]  |[{A, 150}, {C, 250}]|
            |v3       |[{A, 0.01}, {B, 0.02}]  |[{C, 50}, {D, 80}]  |
            +---------+------------------------+--------------------+
            <BLANKLINE>
            >>> df = df.withColumn("cohortMinAlleleCount", FinnGenMetaSummaryStatistics.min_allele_count(f.col("cohortMinAlleleFrequency"), f.col("nCasesPerCohort")))
            >>> df.select("variantId", "cohortMinAlleleCount").show(truncate=False)
            +---------+--------------------+
            |variantId|cohortMinAlleleCount|
            +---------+--------------------+
            |v1       |[{A, 20}, {B, 80}]  |
            |v2       |[{A, 15}]           |
            |v3       |[]                  |
            +---------+--------------------+
            <BLANKLINE>

        """
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
                mac(
                    left.getField("minAlleleFrequency"),
                    f.filter(
                        n_cases_per_cohort,
                        lambda right: right.getField("cohort")
                        == left.getField("cohort"),
                    )
                    .getItem(0)
                    .getField("nCases"),
                ).alias("minAlleleCount"),
            ),
        )

    @classmethod
    def has_low_min_allele_count(
        cls, min_allele_count: Column, min_allele_count_threshold: int = 20
    ) -> Column:
        """Find if variant has a low minor allele count in any of the cohorts.

        Note:
            If any cohort has a minor allele count below the threshold, the variant is considered to have a low minor allele count.


        Args:
            min_allele_count (Column): Column containing array of structs with `cohort` and `minAlleleCount` fields.
            min_allele_count_threshold (int): Threshold below which the minor allele count is considered low.

        Returns:
            Column: Boolean column indicating if any cohort has a low minor allele count.

        Examples:
            >>> data = [("v1", [{"cohort": "A", "minAlleleCount": 30}, {"cohort": "B", "minAlleleCount": 25}]),
            ...         ("v2", [{"cohort": "A", "minAlleleCount": 15}, {"cohort": "B", "minAlleleCount": 25}]),
            ...         ("v3", [{"cohort": "A", "minAlleleCount": 30}, {"cohort": "B", "minAlleleCount": 10}]),
            ...         ("v4", [{"cohort": "A", "minAlleleCount": 5},],)]
            >>> schema = "variantId STRING, cohortMinAlleleCount ARRAY<STRUCT<cohort: STRING, minAlleleCount: INT>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("hasLowMinAlleleCount", FinnGenMetaSummaryStatistics.has_low_min_allele_count(f.col("cohortMinAlleleCount"), 20))
            >>> df.select("variantId", "hasLowMinAlleleCount").show()
            +---------+--------------------+
            |variantId|hasLowMinAlleleCount|
            +---------+--------------------+
            |       v1|               false|
            |       v2|                true|
            |       v3|                true|
            |       v4|                true|
            +---------+--------------------+
            <BLANKLINE>

        """
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
    def has_low_imputation_score(cls, imputation_threshold: float) -> Column:
        """Find if variant has a low r2 imputation score in any of the MVP cohorts.

        Note:
            A missing imputation score is considered as passing the threshold, since it means that the variant was not
            present in that cohort.

        Note:
            If any r2 imputation score is below the threshold, the variant is considered to have a low imputation score.

        Args:
            imputation_threshold (float): Threshold below which the imputation score is considered low.

        Returns:
            Column: Boolean column indicating if any cohort has a low imputation score.

        Examples:
            >>> data = [("v1", 0.9, 0.8, 1.0), ("v2",0.7, 0.9, 0.9), ("v3", None, None, 0.8), ("v4", None, None, 0.7)]
            >>> schema = "variantId STRING, MVP_EUR_r2 DOUBLE, MVP_AFR_r2 DOUBLE, MVP_HIS_r2 DOUBLE"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("hasLowImputationScore", FinnGenMetaSummaryStatistics.has_low_imputation_score(0.8))
            >>> df.select("variantId", "hasLowImputationScore").show()
            +---------+---------------------+
            |variantId|hasLowImputationScore|
            +---------+---------------------+
            |       v1|                false|
            |       v2|                 true|
            |       v3|                false|
            |       v4|                 true|
            +---------+---------------------+
            <BLANKLINE>

        """
        return (
            f.size(
                f.filter(
                    f.array(
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
                    ),
                    lambda x: ~x.getField("filter"),
                )
            )
            > 0
        ).alias("hasLowImputationScore")

    @classmethod
    def cohorts(cls) -> Column:
        """Cohorts involved in the meta-analysis.

        This method creates an array of structs containing biobank and cohort information
        for variants that have allele frequency data available in each respective cohort.

        Returns:
            Column: Array of structs with fields 'biobank' and 'cohort'.

        Examples:
            # Test case 1: All cohorts have data
            >>> data1 = [(0.3, 0.2, 0.4, 0.1, 0.25)]
            >>> schema1 = "MVP_EUR_af_alt DOUBLE, MVP_AFR_af_alt DOUBLE, MVP_HIS_af_alt DOUBLE, fg_af_alt DOUBLE, ukbb_af_alt DOUBLE"
            >>> df1 = spark.createDataFrame(data1, schema1)
            >>> df1.withColumn("cohorts", FinnGenMetaSummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
            +----------------------------------------------------------------------------------+
            |cohorts                                                                           |
            +----------------------------------------------------------------------------------+
            |[{MVP, MVP_EUR}, {MVP, MVP_AFR}, {MVP, MVP_AMR}, {FinnGen, FinnGen}, {UKBB, UKBB}]|
            +----------------------------------------------------------------------------------+
            <BLANKLINE>

            # Test case 2: Only some cohorts have data
            >>> data2 = [(0.3, None, None, 0.1, None)]
            >>> df2 = spark.createDataFrame(data2, schema1)
            >>> df2.withColumn("cohorts", FinnGenMetaSummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
            +------------------------------------+
            |cohorts                             |
            +------------------------------------+
            |[{MVP, MVP_EUR}, {FinnGen, FinnGen}]|
            +------------------------------------+
            <BLANKLINE>
        """
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

        Note:
            if the same biobank has multiple cohorts, it still counts as one biobank.

        Args:
            cohorts (Column): Array of structs with fields 'biobank'.

        Returns:
            Column: Boolean column indicating if the variant is meta-analyzed.

        Examples:
            >>> data = [([("FinnGen", "FinnGen"), ("MVP", "MVP_EUR"), ("MVP", "MVP_AMR")],), ([("MVP", "MVP_AMR"), ("MVP", "MVP_EUR")],),([("UKBB", "UKBB")],)]
            >>> schema = "cohorts ARRAY<STRUCT<biobank: STRING, cohort: STRING>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +----------------------------------------------------+
            |cohorts                                             |
            +----------------------------------------------------+
            |[{FinnGen, FinnGen}, {MVP, MVP_EUR}, {MVP, MVP_AMR}]|
            |[{MVP, MVP_AMR}, {MVP, MVP_EUR}]                    |
            |[{UKBB, UKBB}]                                      |
            +----------------------------------------------------+
            <BLANKLINE>

            >>> df.withColumn("isMetaAnalyzedVariant", FinnGenMetaSummaryStatistics.is_meta_analyzed_variant(f.col("cohorts"))).show(truncate=False)
            +----------------------------------------------------+---------------------+
            |cohorts                                             |isMetaAnalyzedVariant|
            +----------------------------------------------------+---------------------+
            |[{FinnGen, FinnGen}, {MVP, MVP_EUR}, {MVP, MVP_AMR}]|true                 |
            |[{MVP, MVP_AMR}, {MVP, MVP_EUR}]                    |false                |
            |[{UKBB, UKBB}]                                      |false                |
            +----------------------------------------------------+---------------------+
            <BLANKLINE>


        """
        n_biobanks = f.size(
            f.array_distinct(f.transform(cohorts, lambda x: x.getField("biobank"))),
        )
        return (n_biobanks > 1).alias("isMetaAnalyzedVariant")
