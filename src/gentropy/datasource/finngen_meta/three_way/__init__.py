"""Summary-statistics conversion and harmonisation for FinnGen-UKBB-MVP meta-analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import (
    combined_allele_frequency,
    flag_equal_alleles,
    flag_multiallelics,
    flag_non_atgc_alleles,
    flag_star_allele,
    normalize_af,
    normalize_chromosome,
)
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.study_index import MetaAnalysisStudyIndex
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen_meta import (
    FinnGenMetaRelease,
    MetaAnalysisHarmonisationConfig,
    MetaAnalysisType,
    convert_bgzip_to_parquet,
    has_low_min_allele_count,
    has_low_min_allele_frequency,
    min_allele_count,
    min_allele_frequency,
)

THREE_WAY_SUMMARY_STATISTICS_SCHEMA = t.StructType(
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
# Raw FinnGen-UKBB-MVP meta-analysis summary-statistics schema.


class ThreeWaySummaryStatistics:
    """Convert and harmonise three-way FinnGen-UKBB-MVP summary statistics."""

    N_THREAD_MAX = 32
    N_THREAD_OPTIMAL = 10

    @classmethod
    def bgzip_to_parquet(
        cls,
        session: Session,
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        finngen_release: FinnGenMetaRelease,
        meta_analysis_type: MetaAnalysisType,
        n_threads: int = 10,
    ) -> None:
        """Convert gzipped summary statistics to Parquet format.

        This is a pre-step that needs to be performed once to convert the block gzipped to parquet format. This step
        should be run before the actual harmonisation step is performed and the output Parquet files can be used as input
        for the `from_source` method.

        Args:
            session (Session): Session object.
            summary_statistics_list (list[str]): List of GCS paths to gzipped summary statistics files.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
            finngen_release (FinnGenMetaRelease): FinnGen release identifier (e.g. ``"R12"``).
            meta_analysis_type (MetaAnalysisType): Type of meta-analysis; must be ``THREE_WAY``
                (FINNGEN × UKBB × MVP).
            n_threads (int): Maximum number of threads to use for ThreadPoolExecutor (default is 10).

        The output requires a single path that will be populated with Parquet files partitioned by `studyId` extracted
        from the input file names.

        !!! note "Block gzipped input files"

            Since the individual summary statistics files are **block gzipped** we use the enhanced bgzip codec for efficient reading.

        !!! note "Reading multiple files with divergent schemas"

            Since the schema for individual summary statistics **is not strictly the same we have to enforce the schema**.

            **_enforcing schema_**
            using the `enforceSchema` option in `spark.read.csv` **does not map columns that exist in the file provided schema**,
            but rather aligns columns positionally, which breaks the column order per individual file.

            **_inferring schema_**
            Attempting to use the `inferSchema` option in `spark.read.csv` while reading multiple files in bulk drops columns, due
            to the random sampling of files to infer the schema. (Files chosen to infer the schema may not contain entire superset of column space.)

            **_manual schema enforcement_**
            The only way to keep the columns in order and use full column superset is to loop over the files with `inferSchema` and manually
            add missing columns with null values casted to expected type. The looping can be parallelized using a **thread pool** (ThreadPoolExecutor)
            with `n_threads` as the maximum load of jobs to spark cluster.

        ??? warning "Performance considerations"
            This function requires a _Session_ with `use_enhanced_bgzip_codec` to be set to True. This function is strongly encouraged to be used in
            a distributed environment.

        Raises:
            KeyError: If `use_enhanced_bgzip_codec` is set to False in the Session configuration.
        """
        convert_bgzip_to_parquet(
            session=session,
            summary_statistics_list=summary_statistics_list,
            raw_summary_statistics_output_path=raw_summary_statistics_output_path,
            finngen_release=finngen_release,
            meta_analysis_type=meta_analysis_type,
            summary_statistics_schema=THREE_WAY_SUMMARY_STATISTICS_SCHEMA,
            extract_phenotype=cls.extract_study_phenotype_from_path,
            n_threads=n_threads,
        )

    @classmethod
    def from_source(  # noqa: C901
        cls,
        raw_summary_statistics: DataFrame,
        variant_direction: VariantDirection,
        # Configuration
        meta_analysis_study_index: MetaAnalysisStudyIndex,
        config: MetaAnalysisHarmonisationConfig,
    ) -> SummaryStatistics:
        """Build the summary statistics dataset from raw summary statistics.

        See original issue to find out more details on the harmonisation logic https://github.com/opentargets/issues/issues/3474

        ??? note "The logic behind the harmonisation"
            1. Broadcast study-level sample sizes from the meta-analysis study index.
            2. Filter malformed alleles and incomplete association statistics.
            3. Optionally remove variants represented by only one biobank, variants
               with low MVP imputation scores, and variants from small studies.
            4. Join to `VariantDirection` using chromosome, range, and variant ID.
            5. Align beta and cohort allele frequencies to the reference orientation.
            6. Calculate the sample-size-weighted effect-allele frequency.
            7. Optionally apply per-cohort MAC and MAF filters.
            8. Apply the standard summary-statistics sanity filter.

        ??? tip "Variant Directionality"
            **Variant Direction**
            By default we:

            1. keep strand-ambiguous variants unless `remove_ambiguous_alleles` is enabled;
            2. align variants found in gnomAD to its reference orientation;
            3. keep unmatched variants unchanged because their orientation cannot be determined.

        ??? note "Important considerations"
            * The input summary statistics are expected to be already parquet formatted and partitioned by `studyId`.
            * MAC and MAF filters are independent and both run when both are enabled.
            * A minimum allele count of 20 at MAF 1e-4 requires 100,000 diploid
            samples in a cohort. This is stringent for very rare variants and
            smaller cohorts, like MVP_AFR (nSamples ~120k) and MVP_HIS (~60k).
            * MVP_HIS cohort has been mapped to admixed American population - see https://www.science.org/doi/10.1126/science.adj1182 for more details.

        Args:
            raw_summary_statistics (DataFrame): Raw summary statistics dataframe,
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
                f.col("direction"),
                f.col("rangeId"),
            )
            # NOTE: repartition("chromosome") produces very uneven partitions,
            # Spark attempts then to fall back to `dynamic partitioning` algorithm
            # which fails after N failures.
            .repartitionByRange(4_000, "chromosome", "rangeId", "variantId")
            .persist()
        )

        sumstats = raw_summary_statistics
        if config.remove_star_alleles:
            sumstats = sumstats.filter(flag_star_allele(f.col("REF"), f.col("ALT")))
        if config.remove_monomorphic_alleles:
            sumstats = sumstats.filter(flag_equal_alleles(f.col("REF"), f.col("ALT")))
        if config.remove_multiallelic_alleles:
            sumstats = sumstats.filter(flag_multiallelics(f.col("REF"), f.col("ALT")))
        if config.verify_atgc:
            sumstats = sumstats.filter(
                flag_non_atgc_alleles(f.col("REF"), f.col("ALT"))
            )

        sumstats = (
            # Pre-select columns that are needed downstream
            # NOTE: full set of columns is not required.
            sumstats.select(
                f.col("studyId"),
                normalize_chromosome(f.col("#CHR")).alias("chromosome"),
                f.col("POS").cast(t.IntegerType()).alias("position"),
                f.upper("REF").alias("referenceAllele"),
                f.upper("ALT").alias("alternateAllele"),
                f.col("all_inv_var_meta_beta").alias("beta"),
                f.col("all_inv_var_meta_sebeta").alias("standardError"),
                f.col("all_inv_var_meta_mlogp").alias("neglogpval"),
                # Other columns
                f.col("fg_af_alt"),
                f.col("MVP_EUR_r2"),
                f.col("MVP_EUR_af_alt"),
                f.col("MVP_AFR_r2"),
                f.col("MVP_AFR_af_alt"),
                f.col("MVP_HIS_r2"),
                f.col("MVP_HIS_af_alt"),
                f.col("ukbb_af_alt"),
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
            # Initial filters based on statistics presence
            .filter(f.col("neglogpval").isNotNull())
            .filter(f.col("beta").isNotNull())
            .filter(f.col("standardError").isNotNull())
            # Annotate with StudyIndex nSamples and nSamplesPerCohort
            # the cases Minor Allele Count and Samples for combined AF calculation
            .join(si_slice, on="studyId", how="left")
        )

        # Keep variants represented by more than one biobank.
        if config.perform_meta_analysis_filter:
            sumstats = sumstats.filter(cls.is_meta_analyzed_variant(cls.cohorts()))

        # Filter out variants with low INFO score
        if config.perform_imputation_score_filter:
            sumstats = sumstats.filter(
                ~cls.has_low_imputation_score(config.imputation_score_threshold)
            )
        if config.perform_samples_size_filter:
            sumstats = sumstats.filter(f.col("nSamples") > config.sample_size_threshold)

        sumstats = (
            # Join with variant direction dataset
            # Keep variants if not found in gnomAD (left join)
            sumstats.repartitionByRange(4_000, "chromosome", "rangeId", "variantId")
            .join(vd_slice, on=["chromosome", "rangeId", "variantId"], how="left")
            # Use originalVariantId (already flipped) or fall back to variantId if not found in gnomAD
            .withColumn(
                "variantId", f.coalesce(f.col("originalVariantId"), f.col("variantId"))
            )
            # Compute allele frequency per cohort and align with direction
            # NOTE: `direction` column represents if variant aligned to gnomAD variant or it's flipped
            # version, the values are `1` - direct, `-1` - flipped
            .withColumn(
                "cohortAlleleFrequency", cls.allele_frequencies(f.col("direction"))
            )
            # Align beta to the reference direction; preserve unmatched variants.
            .withColumn(
                "beta", f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))
            )
            # Calculate the combined effect allele frequency from cohorts
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
            Assumes the file name format is some_path/to/<studyPhenotype>_meta_out.tsv.gz


        Args:
            file_path (Column): Column containing the file path as a string.

        Returns:
            Column: Extracted study phenotype as a string.

        Examples:
            >>> data = [("/path/to/AB1_meta_out.tsv.gz",), ("/another/path/CD2_meta_out.tsv.gz",)]
            >>> schema = "filePath STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> df = df.withColumn("studyPhenotype", ThreeWaySummaryStatistics.extract_study_phenotype_from_path(f.col("filePath")))
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

    @classmethod
    def allele_frequencies(cls, flip: Column, scale: int = 10) -> Column:
        """Extract the allele frequencies per cohort.

        Note:
            if the `flip` column is -1, then the allele frequency is flipped (1 - af).

        Args:
            flip (Column): Direction column indicating if the allele needs to be flipped. (-1 for flip, 1 for no flip, null for no information)
            scale (int): Scale for the decimal type conversion. Default is 10.

        Returns:
            Column: Column containing array of structs with `cohort` and `alleleFrequency` fields.


        Examples:
            >>> data = [("v1", 0.1, 0.2, None, 0.3, 0.4, -1),
            ...        ("v2", 0.000000001, 0.999999999, None, None, None,1),
            ...        ("v3", 0.1, 0.1, None, None, None, None),]
            >>> schema = "variantId STRING, MVP_EUR_af_alt DOUBLE, MVP_AFR_af_alt DOUBLE, MVP_HIS_af_alt DOUBLE, fg_af_alt DOUBLE, ukbb_af_alt DOUBLE, flip INT"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+--------------+--------------+--------------+---------+-----------+----+
            |variantId|MVP_EUR_af_alt|MVP_AFR_af_alt|MVP_HIS_af_alt|fg_af_alt|ukbb_af_alt|flip|
            +---------+--------------+--------------+--------------+---------+-----------+----+
            |v1       |0.1           |0.2           |NULL          |0.3      |0.4        |-1  |
            |v2       |1.0E-9        |0.999999999   |NULL          |NULL     |NULL       |1   |
            |v3       |0.1           |0.1           |NULL          |NULL     |NULL       |NULL|
            +---------+--------------+--------------+--------------+---------+-----------+----+
            <BLANKLINE>

            >>> df = df.withColumn("alleleFrequencies", ThreeWaySummaryStatistics.allele_frequencies(f.col("flip")))
            >>> df.select("alleleFrequencies").show(truncate=False)
            +-------------------------------------------------------------------------------------------------+
            |alleleFrequencies                                                                                |
            +-------------------------------------------------------------------------------------------------+
            |[{MVP_EUR, 0.9000000000}, {MVP_AFR, 0.8000000000}, {FinnGen, 0.7000000000}, {UKBB, 0.6000000000}]|
            |[{MVP_EUR, 0.0000000010}, {MVP_AFR, 0.9999999990}]                                               |
            |[{MVP_EUR, 0.1000000000}, {MVP_AFR, 0.1000000000}]                                               |
            +-------------------------------------------------------------------------------------------------+
            <BLANKLINE>

        """
        precision = scale + 1  # to ensure we can represent values like 1.0000
        return f.filter(
            f.array(
                f.struct(
                    f.lit("MVP_EUR").alias("cohort"),
                    normalize_af(f.col("MVP_EUR_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    normalize_af(f.col("MVP_AFR_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    # Note: HIS in sumstats is AMR in study index
                    normalize_af(f.col("MVP_HIS_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("FinnGen").alias("cohort"),
                    normalize_af(f.col("fg_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("UKBB").alias("cohort"),
                    normalize_af(f.col("ukbb_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
            ),
            lambda x: x.getField("alleleFrequency").isNotNull(),
        ).alias("alleleFrequencies")

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
            >>> df = df.withColumn("hasLowImputationScore", ThreeWaySummaryStatistics.has_low_imputation_score(0.8))
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
            f.when(
                f.col("MVP_EUR_r2").isNotNull()
                & (f.col("MVP_EUR_r2") < imputation_threshold),
                True,
            )
            .when(
                f.col("MVP_AFR_r2").isNotNull()
                & (f.col("MVP_AFR_r2") < imputation_threshold),
                True,
            )
            .when(
                f.col("MVP_HIS_r2").isNotNull()
                & (f.col("MVP_HIS_r2") < imputation_threshold),
                True,
            )
            .otherwise(False)
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
            >>> df1.withColumn("cohorts", ThreeWaySummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
            +----------------------------------------------------------------------------------+
            |cohorts                                                                           |
            +----------------------------------------------------------------------------------+
            |[{MVP, MVP_EUR}, {MVP, MVP_AFR}, {MVP, MVP_AMR}, {FinnGen, FinnGen}, {UKBB, UKBB}]|
            +----------------------------------------------------------------------------------+
            <BLANKLINE>

            # Test case 2: Only some cohorts have data
            >>> data2 = [(0.3, None, None, 0.1, None)]
            >>> df2 = spark.createDataFrame(data2, schema1)
            >>> df2.withColumn("cohorts", ThreeWaySummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
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

            >>> df.withColumn("isMetaAnalyzedVariant", ThreeWaySummaryStatistics.is_meta_analyzed_variant(f.col("cohorts"))).show(truncate=False)
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
