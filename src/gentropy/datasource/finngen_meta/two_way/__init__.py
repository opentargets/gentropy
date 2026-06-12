"""Summary statistics for two-way meta-analysis (FINNGEN × UKBB)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import SummaryStatistics
from gentropy.common.processing import (
    combined_allele_frequency,
    flag_equal_alleles,
    flag_non_atgc_alleles,
    normalize_af,
    normalize_chromosome,
)
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.study_index import MetaAnalysisStudyIndex
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen_meta import (
    N_THREAD_OPTIMAL,
    FinnGenMetaRelease,
    MetaAnalysisHarmonisationConfig,
    MetaAnalysisType,
    convert_bgzip_to_parquet,
    has_low_min_allele_count,
    has_low_min_allele_frequency,
    min_allele_count,
    min_allele_frequency,
)

if TYPE_CHECKING:
    from gentropy.common.session import Session

TWO_WAY_SUMMARY_STATISTICS_SCHEMA = t.StructType(
    [
        t.StructField("#CHR", t.StringType(), nullable=False),
        t.StructField("POS", t.IntegerType(), nullable=False),
        t.StructField("REF", t.StringType(), nullable=False),
        t.StructField("ALT", t.StringType(), nullable=False),
        t.StructField("SNP", t.StringType(), nullable=False),
        t.StructField("FINNGEN_beta", t.DoubleType(), nullable=True),
        t.StructField("FINNGEN_sebeta", t.DoubleType(), nullable=True),
        t.StructField("FINNGEN_pval", t.DoubleType(), nullable=True),
        t.StructField("FINNGEN_af_alt", t.DoubleType(), nullable=True),
        t.StructField("FINNGEN_af_alt_cases", t.DoubleType(), nullable=True),
        t.StructField("FINNGEN_af_alt_controls", t.DoubleType(), nullable=True),
        t.StructField("UKBB_beta", t.DoubleType(), nullable=True),
        t.StructField("UKBB_sebeta", t.DoubleType(), nullable=True),
        t.StructField("UKBB_pval", t.DoubleType(), nullable=True),
        t.StructField("UKBB_af_alt", t.DoubleType(), nullable=True),
        t.StructField("all_meta_N", t.IntegerType(), nullable=True),
        t.StructField("all_inv_var_meta_beta", t.DoubleType(), nullable=True),
        t.StructField("all_inv_var_meta_sebeta", t.DoubleType(), nullable=True),
        t.StructField("all_inv_var_meta_p", t.DoubleType(), nullable=True),
        t.StructField("all_inv_var_meta_mlogp", t.DoubleType(), nullable=True),
        t.StructField("all_inv_var_het_p", t.DoubleType(), nullable=True),
        t.StructField("leave_FINNGEN_N", t.IntegerType(), nullable=True),
        t.StructField("leave_FINNGEN_inv_var_meta_beta", t.DoubleType(), nullable=True),
        t.StructField(
            "leave_FINNGEN_inv_var_meta_sebeta", t.DoubleType(), nullable=True
        ),
        t.StructField("leave_FINNGEN_inv_var_meta_p", t.DoubleType(), nullable=True),
        t.StructField(
            "leave_FINNGEN_inv_var_meta_mlogp", t.DoubleType(), nullable=True
        ),
        t.StructField(
            "leave_FINNGEN_inv_var_meta_het_p", t.DoubleType(), nullable=True
        ),
        t.StructField("leave_UKBB_N", t.IntegerType(), nullable=True),
        t.StructField("leave_UKBB_inv_var_meta_beta", t.DoubleType(), nullable=True),
        t.StructField("leave_UKBB_inv_var_meta_sebeta", t.DoubleType(), nullable=True),
        t.StructField("leave_UKBB_inv_var_meta_p", t.DoubleType(), nullable=True),
        t.StructField("leave_UKBB_inv_var_meta_mlogp", t.DoubleType(), nullable=True),
        t.StructField("leave_UKBB_inv_var_meta_het_p", t.DoubleType(), nullable=True),
        t.StructField("rsid", t.StringType(), nullable=True),
    ]
)


class TwoWaySummaryStatistics(SummaryStatistics):
    """Summary statistics for two-way meta-analysis (FINNGEN × UKBB)."""

    @classmethod
    def bgzip_to_parquet(
        cls,
        session: Session,
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        finngen_release: FinnGenMetaRelease,
        n_threads: int = N_THREAD_OPTIMAL,
    ) -> None:
        """Convert gzipped two-way summary statistics to Parquet partitioned by ``studyId``.

        This is a pre-step that converts the block-gzipped FinnGen-UKBB files to
        Parquet, stamping ``studyId`` as a partition column so that the
        harmonisation step (`from_source`) can read it directly rather than
        re-deriving it from the original file name.

        Args:
            session (Session): Session object (requires `use_enhanced_bgzip_codec=True`).
            summary_statistics_list (list[str]): List of paths to gzipped summary statistics files.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
            finngen_release (FinnGenMetaRelease): FinnGen release identifier (e.g. ``"R12"``).
            n_threads (int): Maximum number of threads for the ThreadPoolExecutor (default 10).
        """
        convert_bgzip_to_parquet(
            session=session,
            summary_statistics_list=summary_statistics_list,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            finngen_release=finngen_release,
            meta_analysis_type=MetaAnalysisType.TWO_WAY,
            summary_statistics_schema=TWO_WAY_SUMMARY_STATISTICS_SCHEMA,
            extract_phenotype=cls.extract_study_phenotype_from_path,
            n_threads=n_threads,
        )

    @classmethod
    def from_source(  # noqa: C901
        cls,
        raw_summary_statistics: DataFrame,
        variant_direction: VariantDirection,
        meta_analysis_study_index: MetaAnalysisStudyIndex,
        config: MetaAnalysisHarmonisationConfig,
    ) -> SummaryStatistics:
        """Harmonise raw two-way FinnGen-UKBB meta-analysis summary statistics.

        ??? note "The logic behind the harmonisation"
            1. Broadcast study-level sample sizes from the meta-analysis study index.
            2. Filter malformed alleles (star, monomorphic, multiallelic, non-ATGC)
               according to `config`.
            3. Read ``studyId`` from the Parquet partition column (written by
               `bgzip_to_parquet`) and drop rows without complete association statistics.
            4. Optionally remove variants represented by only one biobank and
               variants from small studies.
            5. Join to `VariantDirection` using chromosome, range, and variant ID.
            6. Align beta and cohort allele frequencies to the reference orientation.
            7. Calculate the sample-size-weighted effect-allele frequency.
            8. Optionally apply per-cohort MAC and MAF filters.
            9. Apply the standard summary-statistics sanity filter.

        ??? tip "Variant Directionality"
            By default we keep strand-ambiguous variants unless
            `remove_ambiguous_alleles` is enabled, align variants found in gnomAD
            to its reference orientation, and keep unmatched variants unchanged
            because their orientation cannot be determined.

        Args:
            raw_summary_statistics (DataFrame): Raw two-way summary statistics,
                expected to be Parquet partitioned by ``studyId`` (produced by `bgzip_to_parquet`).
            variant_direction (VariantDirection): Variant direction dataset used for allele alignment.
            meta_analysis_study_index (MetaAnalysisStudyIndex): FinnGen meta-analysis study index.
            config (MetaAnalysisHarmonisationConfig): Configuration for the harmonisation.

        Returns:
            SummaryStatistics: Processed summary statistics dataset.
        """
        si_slice = f.broadcast(
            meta_analysis_study_index.df.select(
                f.col("studyId"),
                f.col("nSamples"),
                f.col("nSamplesPerCohort"),
            )
        )
        vd_slice = variant_direction.df
        if config.remove_ambiguous_alleles:
            vd_slice = vd_slice.filter(~f.col("isStrandAmbiguous"))
        vd_slice = (
            vd_slice.select(
                f.col("chromosome"),
                f.col("originalVariantId"),
                f.col("variantId"),
                f.col("rangeId"),
                f.col("direction"),
            )
            # NOTE: repartition("chromosome") produces very uneven partitions,
            # Spark attempts then to fall back to `dynamic partitioning` algorithm
            # which fails after N failures.
            .repartitionByRange(4_000, "chromosome", "rangeId", "variantId")
            .persist()
        )

        sumstats = raw_summary_statistics
        if config.remove_monomorphic_alleles:
            sumstats = sumstats.filter(flag_equal_alleles(f.col("REF"), f.col("ALT")))
        if config.verify_atgc:
            sumstats = sumstats.filter(
                flag_non_atgc_alleles(f.col("REF"), f.col("ALT"))
            )

        sumstats = (
            # studyId is read from the Parquet partition column written by
            # bgzip_to_parquet; it must not be re-derived from input_file_name()
            # because the original BGZIP file name is lost after conversion.
            sumstats.select(
                f.col("studyId"),
                normalize_chromosome(f.col("#CHR")).alias("chromosome"),
                f.col("POS").alias("position"),
                f.upper("REF").alias("referenceAllele"),
                f.upper("ALT").alias("alternateAllele"),
                f.col("all_inv_var_meta_beta").alias("beta"),
                f.col("all_inv_var_meta_sebeta").alias("standardError"),
                f.col("all_inv_var_meta_mlogp").alias("neglogpval"),
                # Other columns
                f.col("FINNGEN_af_alt"),
                f.col("UKBB_af_alt"),
                f.col("all_meta_N"),
                f.floor(f.col("POS") / config.flipping_window_size)
                .cast(t.IntegerType())
                .alias("rangeId"),
            )
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
            .drop("referenceAllele", "alternateAllele")
            # Filters based on statistics presence
            .filter(f.col("neglogpval").isNotNull())
            .filter(f.col("beta").isNotNull())
            .filter(f.col("standardError").isNotNull())
            .join(si_slice, on="studyId", how="left")
        )
        if config.perform_meta_analysis_filter:
            sumstats = sumstats.filter(cls.is_meta_analyzed_variant())
        if config.perform_samples_size_filter:
            sumstats = sumstats.filter(f.col("nSamples") > config.sample_size_threshold)

        sumstats = (
            sumstats.repartitionByRange(4_000, "chromosome", "rangeId", "variantId")
            .join(vd_slice, on=["chromosome", "rangeId", "variantId"], how="left")
            .withColumn(
                "variantId", f.coalesce(f.col("originalVariantId"), f.col("variantId"))
            )
            .withColumn(
                "beta", f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))
            )
            .withColumn(
                "cohortAlleleFrequency", cls.allele_frequencies(f.col("direction"))
            )
            .withColumn(
                "effectAlleleFrequencyFromSource",
                combined_allele_frequency(
                    f.col("cohortAlleleFrequency"), f.col("nSamplesPerCohort")
                ),
            )
        )

        if (
            config.perform_min_allele_count_filter
            or config.perform_min_allele_frequency_filter
        ):
            # Calculate the MAF per cohort
            sumstats = sumstats.withColumn(
                "cohortMinAlleleFrequency",
                min_allele_frequency(f.col("cohortAlleleFrequency")),
            )
        if config.perform_min_allele_count_filter:
            sumstats = (
                sumstats
                # Make sure to only keep cohorts that have nSamples > 0
                .withColumn(
                    "nSamplesPerCohort",
                    f.filter(
                        f.col("nSamplesPerCohort"),
                        lambda x: (
                            x.getField("nSamples").isNotNull()
                            & (x.getField("nSamples") > 0)
                        ),
                    ),
                )
                .withColumn(
                    "cohortMinAlleleCount",
                    min_allele_count(
                        f.col("cohortMinAlleleFrequency"),
                        f.col("nSamplesPerCohort"),
                    ),
                )
                .withColumn(
                    "hasLowMinAlleleCount",
                    has_low_min_allele_count(
                        f.col("cohortMinAlleleCount"),
                        config.min_allele_count_threshold,
                    ),
                )
                .filter(~f.col("hasLowMinAlleleCount"))
                .drop("hasLowMinAlleleCount", "cohortMinAlleleCount")
            )
        # Apply the optional MAF filter.
        if config.perform_min_allele_frequency_filter:
            sumstats = (
                sumstats.withColumn(
                    "hasLowMinAlleleFrequency",
                    has_low_min_allele_frequency(
                        f.col("cohortMinAlleleFrequency"),
                        config.min_allele_frequency_threshold,
                    ),
                )
                .filter(~f.col("hasLowMinAlleleFrequency"))
                .drop("hasLowMinAlleleFrequency")
            )
        # Convert to final summary statistics schema
        sumstats = sumstats.select(
            f.col("studyId"),
            f.col("variantId"),
            f.col("chromosome"),
            f.col("position"),
            f.col("beta"),
            f.col("nSamples").alias("sampleSize"),
            *pvalue_from_neglogpval(f.col("neglogpval")),
            f.col("effectAlleleFrequencyFromSource"),
            f.col("standardError"),
        )

        return SummaryStatistics(sumstats).sanity_filter()

    @staticmethod
    def extract_study_phenotype_from_path(file_path: Column) -> Column:
        """Extract the study phenotype from FinnGen file path.

        Note:
            Assumes the file name format is some_path/to/finngen_R<release>_<studyPhenotype>_meta_out.tsv.gz


        Args:
            file_path (Column): Column containing the file path as a string.

        Returns:
            Column: Extracted study phenotype as a string.

        Examples:
            >>> data = [("finngen_R12_T2D_WIDE_meta_out.tsv.gz",),]
            >>> schema = "filePath STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("studyPhenotype", TwoWaySummaryStatistics.extract_study_phenotype_from_path(f.col("filePath")))
            >>> df.show(truncate=False)
            +------------------------------------+--------------+
            |filePath                            |studyPhenotype|
            +------------------------------------+--------------+
            |finngen_R12_T2D_WIDE_meta_out.tsv.gz|T2D_WIDE      |
            +------------------------------------+--------------+
            <BLANKLINE>

        """
        return f.regexp_replace(
            f.regexp_replace(
                f.element_at(f.split(file_path, "/"), -1), "_meta_out.tsv.gz", ""
            ),
            r"finngen_R\d+_",
            "",
        )

    @classmethod
    def allele_frequencies(cls, flip: Column, scale: int = 10) -> Column:
        """Calculate the cohort-specific allele frequencies based on the variant direction.

        Note:
            if the `flip` column is -1, then the allele frequency is flipped (1 - af).

        Args:
            flip (Column): Direction column indicating if the allele needs to be flipped. (-1 for flip, 1 for no flip, null for no information)
            scale (int): Scale for the decimal type conversion. Default is 10.

        Returns:
            Column: Cohort-specific allele frequencies.

        Examples:
            >>> data = [("v1", 0.1, 0.2, 1), ("v2", 0.1, 0.2, -1), ("v3", None, 0.2, None)]
            >>> schema = "variantId STRING, UKBB_af_alt DOUBLE, FINNGEN_af_alt DOUBLE, direction INTEGER"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("cohortAlleleFrequency", TwoWaySummaryStatistics.allele_frequencies(f.col("direction")))
            >>> df.select("variantId", "cohortAlleleFrequency").show(truncate=False)
            +---------+-----------------------------------------------+
            |variantId|cohortAlleleFrequency                          |
            +---------+-----------------------------------------------+
            |v1       |[{UKBB, 0.1000000000}, {FinnGen, 0.2000000000}]|
            |v2       |[{UKBB, 0.9000000000}, {FinnGen, 0.8000000000}]|
            |v3       |[{FinnGen, 0.2000000000}]                      |
            +---------+-----------------------------------------------+
            <BLANKLINE>

        """
        precision = scale + 1
        return f.filter(
            f.array(
                f.struct(
                    f.lit("UKBB").alias("cohort"),
                    normalize_af(f.col("UKBB_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("FinnGen").alias("cohort"),
                    normalize_af(f.col("FINNGEN_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
            ),
            lambda x: x.getField("alleleFrequency").isNotNull(),
        )

    @classmethod
    def is_meta_analyzed_variant(cls) -> Column:
        """Check if the variant is meta-analyzed in at least 2 cohorts.

        Examples:
            >>> data = [("v1", 2), ("v2", 1), ("v3", 1), ("v4", 1)]
            >>> schema = "variantId STRING, all_meta_N INTEGER"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("isMetaAnalyzedVariant", TwoWaySummaryStatistics.is_meta_analyzed_variant())
            >>> df.show(truncate=False)
            +---------+----------+---------------------+
            |variantId|all_meta_N|isMetaAnalyzedVariant|
            +---------+----------+---------------------+
            |v1       |2         |true                 |
            |v2       |1         |false                |
            |v3       |1         |false                |
            |v4       |1         |false                |
            +---------+----------+---------------------+
            <BLANKLINE>

        """
        return (f.col("all_meta_N") == 2).alias("isMetaAnalyzedVariant")
