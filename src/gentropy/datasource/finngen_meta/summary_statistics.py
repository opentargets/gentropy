"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy.common.processing import mac, maf, normalize_chromosome
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.finngen_meta import FinnGenMetaManifest, MetaAnalysisDataSource


class FinnGenUkbMvpMetaSummaryStatistics:
    """FinnGen meta summary statistics ingestion and harmonisation."""

    N_THREAD_MAX = 32
    N_THREAD_OPTIMAL = 10

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
            >>> df = df.withColumn("studyPhenotype", FinnGenUkbMvpMetaSummaryStatistics.extract_study_phenotype_from_path(f.col("filePath")))
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
        n_threads: int = 10,
    ) -> None:
        """Convert gzipped summary statistics to Parquet format.

        This is a pre-step that needs to be performed once to convert the block gzipped to parquet format. This step
        should be run before the actual harmonisation step is performed and the output Parquet files can be used as input
        for the `from_source` method.

        Args:
            session (Session): Session object.
            summary_statistics_list (list[str]): List of paths where summary statistics files are located.
            datasource (MetaAnalysisDataSource): Data source, can be FinnGenMetaDataSource.FINNGEN_UKBB_MVP.
            raw_summary_statistics_output_path (str): Output path for the Parquet files.
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
        if len(summary_statistics_list) == 0:
            session.logger.warning("No summary statistics paths found to process.")
            return
        if not session.use_enhanced_bgzip_codec:
            session.logger.error(
                "The use_enhanced_bgzip_codec is set to False. This will lead to inefficient reading of block gzipped files."
            )
            raise KeyError(
                "Please set `session.spark.use_enhanced_bgzip_codec` to True in the Session configuration."
            )

        # Handle n_threads limits and warnings
        if not isinstance(n_threads, int) or n_threads < 1:
            session.logger.warning(
                f"Invalid n_threads value: {n_threads}. Falling back to 10 threads."
            )
            n_threads = FinnGenUkbMvpMetaSummaryStatistics.N_THREAD_OPTIMAL
        if n_threads < FinnGenUkbMvpMetaSummaryStatistics.N_THREAD_OPTIMAL:
            session.logger.warning(
                f"Using low n_threads value: {n_threads}. This may lead to sub-optimal performance."
            )
        if n_threads > FinnGenUkbMvpMetaSummaryStatistics.N_THREAD_MAX:
            session.logger.warning(
                f"Using high n_threads value: {n_threads}, this may lead to overloading spark driver. Limiting to 32."
            )
            n_threads = FinnGenUkbMvpMetaSummaryStatistics.N_THREAD_MAX

        def process_one(
            input_path: str, session: Session, output_path: str
        ) -> DataFrame:
            """Function to process one finngen-ukbb-mvp summary statistics file to schema superset.

            Args:
                input_path (str): Input path to the gzipped summary statistics file.
                session (Session): Session object.
                output_path (str): Output path for the Parquet files.

            Returns:
                DataFrame: Processed dataframe.
            """
            df = session.spark.read.csv(
                input_path,
                header=True,
                inferSchema=True,
                sep="\t",
                enforceSchema=False,
            )
            # Apply schema enforcement by selecting columns in the expected order with proper types
            # NOTE: Here we only add missing columns with null values casted to expected type
            for c in cls.raw_schema.names:
                if c not in df.columns:
                    df = df.withColumn(c, f.lit(None).cast(cls.raw_schema[c].dataType))

            # Replace all NA with nulls and cast to the expected type
            # NOTE: Here we apply the transformation on full schema set (including added missing columns)
            # NOTE: If we do not transform the `NA` to nulls, we will still have divergent schemas, as `NA` forces the column to StringType
            for field in cls.raw_schema.fields:
                df = df.withColumn(
                    field.name,
                    f.when(f.col(field.name) == "NA", f.lit(None))
                    .otherwise(f.col(field.name))
                    .cast(field.dataType),
                )
            # Add studyId based on the input path
            df = (
                df.withColumn(
                    "studyId",
                    f.concat_ws(
                        "_",
                        f.lit(datasource.value),
                        f.lit(
                            cls.extract_study_phenotype_from_path(f.input_file_name())
                        ),
                    ),
                    # Optimal partition size is ~ 100MB, assuming the total size of the dataset is 2Tb
                    # we can have up to 60 partitions per study (330 studies)
                )
                .orderBy("studyId", "#CHR", "POS")
                .repartition(60, "#CHR", "POS")
            )
            # Write out the processed dataframe to Parquet
            # NOTE: Write is done per studyId partition from the thread pool to
            # make sure we do not need to collect all data after the thread execution.
            df.write.mode("append").partitionBy("studyId").parquet(output_path)
            return df

        session.logger.info(
            f"Converting gzipped summary statistics from {summary_statistics_list} to Parquet at {raw_summary_statistics_output_path}."
        )
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
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
        perform_min_allele_count_filter: bool = True,
        min_allele_frequency_threshold: float = 1e-4,
        perform_min_allele_frequency_filter: bool = False,
        filter_out_ambiguous_variants: bool = False,
    ) -> SummaryStatistics:
        """Build the summary statistics dataset from raw summary statistics.

        See original issue to find out more details on the harmonisation logic https://github.com/opentargets/issues/issues/3474

        ??? note "The logic behind the harmonisation"
            1. Build a slice of FinnGen Manifest to bring the information about nCases and nSamples per cohort (broadcast join).
            2. Build a variant direction (gnomAD) dataset partitioned by `chromosome` and `variantId` for joining using Sort-Merge strategy.
            3. Select required columns from raw summary statistics
            4. Remove all non-meta analyzed variants (nBiobanks < 2) - by default
            5. Remove all variants with low imputation score - by default
            6. Join with Finngen Manifest and Variant Direction datasets
            7. Flip `beta` and `min allele frequency` based on the `direction` from Variant Direction dataset
            8. Use originalVariantId from Variant Direction dataset if available, otherwise fall back to variantId (for variants missing from Variant Direction)
            9. Calculate combined effect allele frequency from cohorts
            10. Remove all variants with low Min Allele Count - by default
            11. Remove all variants with low Min Allele Frequency - optional
            12. Remove strand ambiguous variants - optional

        ??? tip "Variant Directionality"
            **Variant Direction**
            By default we:

            1. keep all strand ambiguous variants as is
            2. keep all variants found in gnomAD aligned to gnomAD reference ( if alleles are flipped we flip the beta and allele frequency)
            3. keep all variants not found in gnomAD as is (cannot determine strand or alignment) - we assume these are correct.

        ??? note "Important considerations"
            * The input summary statistics are expected to be already parquet formatted and partitioned by `studyId`.
            * Both `perform_allele_count_filter` and `perform_allele_frequency_filter` are redundant, if both
            are set to True, only `perform_allele_count_filter` will be applied.
            * Threshold value for Min Allele Count >= 20 means that with a MAF of 1e-4 we would expect to see
            at least 1_000 samples with minor allele in a cohort. This is quiet stringent for very rare variants and
            smaller cohorts, like MVP_AFR (nSamples ~120k) and MVP_HIS (~60k).
            * MVP_HIS cohort has been mapped to admixed American population - see https://www.science.org/doi/10.1126/science.adj1182 for more details.

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
            filter_out_ambiguous_variants (bool): Whether to filter out strand ambiguous variants. Default is False.

        Returns:
            SummaryStatistics: Processed summary statistics dataset.
        """
        if perform_min_allele_count_filter:
            assert (
                min_allele_count_threshold > 0
            ), "Allele count threshold should be positive."
        if perform_min_allele_frequency_filter:
            assert (
                0.0 <= min_allele_frequency_threshold <= 0.5
            ), "MAF needs to be between 0 and 0.5."

        if perform_min_allele_count_filter and perform_min_allele_frequency_filter:
            # NOTE - MAC filter would be more stringent at low allele frequencies, so no
            # need to have both filters active at the same time
            perform_min_allele_frequency_filter = False

        si_slice = f.broadcast(
            finngen_manifest.df.select(
                f.col("studyId"),
                f.col("nCasesPerCohort"),
                f.col("nSamples"),
                f.col("nSamplesPerCohort"),
            ).persist()
        )
        vd_slice = (
            variant_annotations.df.select(
                f.col("chromosome"),
                f.col("originalVariantId"),
                f.col("variantId"),
                f.col("direction"),
                f.col("isStrandAmbiguous"),
            )
            # NOTE: repartition("chromosome") produces very uneven partitions,
            # Spark attempts then to fall back to `dynamic partitioning` algorithm
            # which fails after N failures.
            .repartitionByRange(4_000, "chromosome", "variantId")
            .persist()
        )

        sumstats = (
            # Pre-select columns that are needed downstream
            # NOTE: full set of columns is not required.
            raw_summary_statistics.select(
                f.col("#CHR"),
                f.col("POS"),
                f.col("REF"),
                f.col("ALT"),
                f.col("fg_af_alt"),
                f.col("MVP_EUR_r2"),
                f.col("MVP_EUR_af_alt"),
                f.col("MVP_AFR_r2"),
                f.col("MVP_AFR_af_alt"),
                f.col("MVP_HIS_r2"),
                f.col("MVP_HIS_af_alt"),
                f.col("ukbb_af_alt"),
                f.col("all_inv_var_meta_mlogp"),
                f.col("all_inv_var_meta_beta"),
                f.col("all_inv_var_meta_sebeta"),
                f.col("studyId"),
            )
            # NOTE: make sure the chromosome is coded to 1:22, X, Y
            .withColumn(
                "chromosome",
                normalize_chromosome(f.col("#CHR").cast(t.StringType())),
            )
            .drop("#CHR")
            .withColumn("position", f.col("POS").cast(t.IntegerType()))
            .withColumnRenamed("REF", "referenceAllele")
            .withColumnRenamed("ALT", "alternateAllele")
            .withColumn(
                "neglogpval", f.col("all_inv_var_meta_mlogp").cast(t.DoubleType())
            )
            .withColumn("beta", f.col("all_inv_var_meta_beta").cast(t.DoubleType()))
            .withColumn(
                "standardError", f.col("all_inv_var_meta_sebeta").cast(t.DoubleType())
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
            assert (
                imputation_score_threshold >= 0.0
            ), "Imputation score threshold should be positive."
            sumstats = (
                sumstats.withColumn(
                    "hasLowImputationScore",
                    cls.has_low_imputation_score(imputation_score_threshold),
                )
                .filter(~f.col("hasLowImputationScore"))
                .drop("hasLowImputationScore", "imputationScore")
            )

        sumstats = (
            # Annotate with StudyIndex nCases, nSamples to obtain
            # the cases Minor Allele Count and Samples for combined AF calculation
            sumstats.join(si_slice, on="studyId", how="left")
            # Join with variant direction dataset
            # Keep variants if not found in gnomAD (left join)
            .repartitionByRange(4_000, "chromosome", "variantId")
            .join(vd_slice, on=["chromosome", "variantId"], how="left")
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
            # Make sure the beta is alined with the direction, if direction is null, keep beta as is
            .withColumn(
                "beta", f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))
            )
            # Calculate the combined effect allele frequency from cohorts
            .withColumn(
                "effectAlleleFrequencyFromSource",
                cls.combined_allele_frequency(
                    f.col("cohortAlleleFrequency"), f.col("nSamplesPerCohort")
                ),
            )
        )
        # Remove strand ambiguous variants, if not found in gnomAD, we keep the variant
        # Not run by default
        if filter_out_ambiguous_variants:
            sumstats = sumstats.filter(
                ~f.coalesce(f.col("isStrandAmbiguous"), f.lit(False))
            )
        if perform_min_allele_count_filter or perform_min_allele_frequency_filter:
            # Calculate the MAF per cohort
            sumstats = sumstats.withColumn(
                "cohortMinAlleleFrequency",
                cls.min_allele_frequency(f.col("cohortAlleleFrequency")),
            )
        if perform_min_allele_count_filter:
            sumstats = (
                sumstats
                # Make sure to only keep cohorts that have nSamples > 0
                .withColumn(
                    "nSamplesPerCohort",
                    f.filter(
                        f.col("nSamplesPerCohort"),
                        lambda x: x.getField("nSamples").isNotNull()
                        & (x.getField("nSamples") > 0),
                    ),
                )
                .withColumn(
                    "cohortMinAlleleCount",
                    cls.min_allele_count(
                        f.col("cohortMinAlleleFrequency"),
                        f.col("nSamplesPerCohort"),
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
        # Not run by default.
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
        ).select(
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

            >>> df = df.withColumn("hasMinAlleleFrequency", FinnGenUkbMvpMetaSummaryStatistics.has_low_min_allele_frequency(f.col("cohortMinAlleleFrequency")))
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
    def normalize_af(cls, af: Column, flip: Column) -> Column:
        """Normalize allele frequency based on variant direction.

        Args:
            af (Column): Allele frequency column.
            flip (Column): Direction column indicating if the allele needs to be flipped.

        Returns:
            Column: Normalized allele frequency.

        Examples:
            >>> data = [("v1", 0.1, 1),
            ...       ("v2", 0.2, -1),
            ...       ("v3", None, -1),
            ...       ("V4", 0.1, None),]
            >>> schema = "variantId STRING, af DOUBLE, flip INT"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+----+----+
            |variantId|af  |flip|
            +---------+----+----+
            |v1       |0.1 |1   |
            |v2       |0.2 |-1  |
            |v3       |NULL|-1  |
            |V4       |0.1 |NULL|
            +---------+----+----+
            <BLANKLINE>

            >>> df = df.withColumn("normalizedAf", FinnGenUkbMvpMetaSummaryStatistics.normalize_af(f.col("af"), f.col("flip")))
            >>> df.select("variantId", "normalizedAf").show(truncate=False)
            +---------+------------+
            |variantId|normalizedAf|
            +---------+------------+
            |v1       |0.1         |
            |v2       |0.8         |
            |v3       |NULL        |
            |V4       |0.1         |
            +---------+------------+
            <BLANKLINE>
        """
        return f.when((flip == -1) & af.isNotNull(), f.lit(1.0) - af).otherwise(af)

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

            >>> df = df.withColumn("alleleFrequencies", FinnGenUkbMvpMetaSummaryStatistics.allele_frequencies(f.col("flip")))
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
                    cls.normalize_af(f.col("MVP_EUR_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    cls.normalize_af(f.col("MVP_AFR_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    # Note: HIS in sumstats is AMR in study index
                    cls.normalize_af(f.col("MVP_HIS_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("FinnGen").alias("cohort"),
                    cls.normalize_af(f.col("fg_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
                f.struct(
                    f.lit("UKBB").alias("cohort"),
                    cls.normalize_af(f.col("ukbb_af_alt"), flip)
                    .cast(t.DecimalType(precision, scale))
                    .alias("alleleFrequency"),
                ),
            ),
            lambda x: x.getField("alleleFrequency").isNotNull(),
        ).alias("alleleFrequencies")

    @classmethod
    def combined_allele_frequency(
        cls, allele_freq: Column, n_samples_per_cohort: Column
    ) -> Column:
        """Combined Allele Frequency across all cohorts.

        Args:
            allele_freq (Column): Column containing array of structs with `cohort` and `alleleFrequency` fields.
            n_samples_per_cohort (Column): Column containing array of structs with `cohort` and `nSamples` fields.

        Returns:
            Column: Combined allele frequency across all cohorts.

        Note:
            The combination is made by weighting the allele frequencies by the number of samples in each cohort.

        Examples:
            >>> data = [
            ...    ("v1", [{"cohort": "A", "alleleFrequency": 0.6}, {"cohort": "B", "alleleFrequency": 0.2}, {"cohort": "C", "alleleFrequency": 0.3}],
            ...           [{"cohort": "A", "nSamples": 100}, {"cohort": "B", "nSamples": 200}, {"cohort": "D", "nSamples": 20}]),
            ...    ("v2", [{"cohort": "A", "alleleFrequency": None},], [{"cohort": "A", "nSamples": 50}]),
            ...    ("v3", [{"cohort": "A", "alleleFrequency": 0.05},], [{"cohort": "A", "nSamples": None}]),]
            >>> schema = "variantId STRING, alleleFrequencies ARRAY<STRUCT<cohort: STRING, alleleFrequency: DOUBLE>>, nSamplesPerCohort ARRAY<STRUCT<cohort: STRING, nSamples: INT>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+------------------------------+-----------------------------+
            |variantId|alleleFrequencies             |nSamplesPerCohort            |
            +---------+------------------------------+-----------------------------+
            |v1       |[{A, 0.6}, {B, 0.2}, {C, 0.3}]|[{A, 100}, {B, 200}, {D, 20}]|
            |v2       |[{A, NULL}]                   |[{A, 50}]                    |
            |v3       |[{A, 0.05}]                   |[{A, NULL}]                  |
            +---------+------------------------------+-----------------------------+
            <BLANKLINE>

            >>> df = df.withColumn("combinedAlleleFrequency", FinnGenUkbMvpMetaSummaryStatistics.combined_allele_frequency(f.col("alleleFrequencies"), f.col("nSamplesPerCohort")))
            >>> df.select("variantId", f.round("combinedAlleleFrequency", 2).alias("caf")).show(truncate=False)
            +---------+----+
            |variantId|caf |
            +---------+----+
            |v1       |0.33|
            |v2       |NULL|
            |v3       |NULL|
            +---------+----+
            <BLANKLINE>
        """
        af_filtered = f.filter(
            allele_freq, lambda x: x.getField("alleleFrequency").isNotNull()
        )
        samples_filtered = f.filter(
            n_samples_per_cohort, lambda x: x.getField("nSamples").isNotNull()
        )

        intersect = f.array_intersect(
            f.transform(af_filtered, lambda x: x.getField("cohort")),
            f.transform(samples_filtered, lambda x: x.getField("cohort")),
        )
        af_map = f.map_from_entries(
            f.filter(
                af_filtered, lambda x: f.array_contains(intersect, x.getField("cohort"))
            )
        )
        n_samples_map = f.map_from_entries(
            f.filter(
                samples_filtered,
                lambda x: f.array_contains(intersect, x.getField("cohort")),
            )
        )
        # Compute numerator: sum(AF * n)
        af_times_n = f.aggregate(
            f.map_entries(af_map),
            f.lit(0.0),
            lambda acc, kv: acc
            + kv["value"]
            * f.coalesce(f.element_at(n_samples_map, kv["key"]), f.lit(0.0)),
        )

        # Compute denominator: sum(n)
        n_total = f.aggregate(
            f.map_entries(n_samples_map),
            f.lit(0),
            lambda acc, kv: acc + f.coalesce(kv["value"], f.lit(0)),
        )

        return (
            f.when((n_total == 0) | (af_times_n == 0), f.lit(None))
            .otherwise(af_times_n / n_total)
            .cast(t.FloatType())
        )

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

            >>> df = df.withColumn("cohortMinAlleleFrequency", FinnGenUkbMvpMetaSummaryStatistics.min_allele_frequency(f.col("alleleFrequencies")))
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
        cls, cohort_min_allele_frequency: Column, n_samples_per_cohort: Column
    ) -> Column:
        """Minor Allele Count (MAC) per cohort.

        Note:
            If a cohort either do not have the maf or nCases, it will be dropped from the resulting MAC array.

        Args:
            cohort_min_allele_frequency (Column): Column containing array of structs with `cohort` and `minAlleleFrequency` fields.
            n_samples_per_cohort (Column): Column containing array of structs with `cohort` and `nSamples` fields.

        Returns:
            Column: Column containing array of structs with `cohort` and `minAlleleCount` fields.

        Examples:
            >>> maf = {"v1": [{"cohort": "A", "minAlleleFrequency": 0.1}, {"cohort": "B", "minAlleleFrequency": 0.2}],
            ...         "v2": [{"cohort": "A", "minAlleleFrequency": 0.05}, {"cohort": "D", "minAlleleFrequency": 0.15}],
            ...         "v3": [{"cohort": "A", "minAlleleFrequency": 0.01}, {"cohort": "B", "minAlleleFrequency": 0.02}],}
            >>> n_samples = {"v1": [{"cohort": "A", "nSamples": 100}, {"cohort": "B", "nSamples": 200}],
            ...            "v2": [{"cohort": "A", "nSamples": 150}, {"cohort": "C", "nSamples": 250}],
            ...            "v3": [{"cohort": "C", "nSamples": 50}, {"cohort": "D", "nSamples": 80}],}
            >>> data = [("v1", maf["v1"], n_samples["v1"]),
            ...         ("v2", maf["v2"], n_samples["v2"]),
            ...         ("v3", maf["v3"], n_samples["v3"]),]
            >>> schema = "variantId STRING, cohortMinAlleleFrequency ARRAY<STRUCT<cohort: STRING, minAlleleFrequency: DOUBLE>>, nSamplesPerCohort ARRAY<STRUCT<cohort: STRING, nSamples: INT>>"
            >>> df = spark.createDataFrame(data, schema)
            >>> df.show(truncate=False)
            +---------+------------------------+--------------------+
            |variantId|cohortMinAlleleFrequency|nSamplesPerCohort   |
            +---------+------------------------+--------------------+
            |v1       |[{A, 0.1}, {B, 0.2}]    |[{A, 100}, {B, 200}]|
            |v2       |[{A, 0.05}, {D, 0.15}]  |[{A, 150}, {C, 250}]|
            |v3       |[{A, 0.01}, {B, 0.02}]  |[{C, 50}, {D, 80}]  |
            +---------+------------------------+--------------------+
            <BLANKLINE>
            >>> df = df.withColumn("cohortMinAlleleCount", FinnGenUkbMvpMetaSummaryStatistics.min_allele_count(f.col("cohortMinAlleleFrequency"), f.col("nSamplesPerCohort")))
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
                    n_samples_per_cohort,
                    lambda right: right.getField("cohort") == left.getField("cohort"),
                ),
            ),
            lambda left: f.struct(
                left.getField("cohort").alias("cohort"),
                mac(
                    left.getField("minAlleleFrequency"),
                    f.filter(
                        n_samples_per_cohort,
                        lambda right: right.getField("cohort")
                        == left.getField("cohort"),
                    )
                    .getItem(0)
                    .getField("nSamples"),
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
            >>> df = df.withColumn("hasLowMinAlleleCount", FinnGenUkbMvpMetaSummaryStatistics.has_low_min_allele_count(f.col("cohortMinAlleleCount"), 20))
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
            >>> df = df.withColumn("hasLowImputationScore", FinnGenUkbMvpMetaSummaryStatistics.has_low_imputation_score(0.8))
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
            >>> df1.withColumn("cohorts", FinnGenUkbMvpMetaSummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
            +----------------------------------------------------------------------------------+
            |cohorts                                                                           |
            +----------------------------------------------------------------------------------+
            |[{MVP, MVP_EUR}, {MVP, MVP_AFR}, {MVP, MVP_AMR}, {FinnGen, FinnGen}, {UKBB, UKBB}]|
            +----------------------------------------------------------------------------------+
            <BLANKLINE>

            # Test case 2: Only some cohorts have data
            >>> data2 = [(0.3, None, None, 0.1, None)]
            >>> df2 = spark.createDataFrame(data2, schema1)
            >>> df2.withColumn("cohorts", FinnGenUkbMvpMetaSummaryStatistics.cohorts()).select("cohorts").show(truncate=False)
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

            >>> df.withColumn("isMetaAnalyzedVariant", FinnGenUkbMvpMetaSummaryStatistics.is_meta_analyzed_variant(f.col("cohorts"))).show(truncate=False)
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
