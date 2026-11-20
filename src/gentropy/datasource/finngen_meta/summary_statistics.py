"""Summary statistics ingestion step for Finngen metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from gentropy.common.session import Session

from functools import reduce

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

    # The columns of the source files that `from_source` reads.
    source_columns = [
        "#CHR",
        "POS",
        "REF",
        "ALT",
        "fg_af_alt",
        "MVP_EUR_r2",
        "MVP_EUR_af_alt",
        "MVP_AFR_r2",
        "MVP_AFR_af_alt",
        "MVP_HIS_r2",
        "MVP_HIS_af_alt",
        "ukbb_af_alt",
        "all_inv_var_meta_mlogp",
        "all_inv_var_meta_beta",
        "all_inv_var_meta_sebeta",
    ]

    @staticmethod
    def read_header(session: Session, path: str) -> list[str]:
        """Read the column names of a summary statistics file from its first line.

        The file is opened with the Hadoop FileSystem of the session, so the path can be
        local or on an object store, and a compressed file is decompressed with the codec
        of its extension. Only the first line is read.

        Args:
            session (Session): Session object.
            path (str): Path to the summary statistics file.

        Returns:
            list[str]: Column names, in the order of the file.

        Raises:
            ValueError: If the file is empty.

        Examples:
            >>> FinnGenUkbMvpMetaSummaryStatistics.read_header(  # doctest: +SKIP
            ...     session, "gs://bucket/summary_stats/AB1_meta_out.tsv.gz"
            ... )
            ['#CHR', 'POS', 'REF', 'ALT', 'SNP', 'fg_beta', ...]
        """
        sc = session.spark.sparkContext
        jvm = sc._jvm
        conf = sc._jsc.hadoopConfiguration()
        jpath = jvm.org.apache.hadoop.fs.Path(path)
        stream = jpath.getFileSystem(conf).open(jpath)
        codec = jvm.org.apache.hadoop.io.compress.CompressionCodecFactory(
            conf
        ).getCodec(jpath)
        if codec is not None:
            stream = codec.createInputStream(stream)
        reader = jvm.java.io.BufferedReader(
            jvm.java.io.InputStreamReader(stream, "UTF-8")
        )
        try:
            line = reader.readLine()
        finally:
            reader.close()
        if line is None:
            raise ValueError(f"Summary statistics file is empty: {path}")
        return line.split("\t")

    @classmethod
    def from_source_files(
        cls,
        session: Session,
        summary_statistics_paths: list[str],
        datasource: MetaAnalysisDataSource,
    ) -> DataFrame:
        """Read the source summary statistics files into the columns `from_source` reads.

        The files of one meta-analysis have different columns: a cohort that a phenotype
        was not analysed in has no columns in its file. The header of each file is read
        first, and the files with the same header are read together, as tab-separated
        text with the column names of that header. The `source_columns` are then cast to
        the types of `raw_schema`, with `NA` read as null, and the `source_columns` a
        header lacks are null. `studyId` is the meta-analysis prefix and the phenotype of
        the file name.

        The files are block gzipped, so they are split across tasks when the session sets
        `use_enhanced_bgzip_codec`. Without it, each file is read by a single task.

        Args:
            session (Session): Session object.
            summary_statistics_paths (list[str]): Paths of the summary statistics files.
            datasource (MetaAnalysisDataSource): Meta-analysis of the files.

        Returns:
            DataFrame: `studyId` and the `source_columns`, one row per variant of a file.

        Raises:
            ValueError: If no path is given.

        Examples:
            >>> df = FinnGenUkbMvpMetaSummaryStatistics.from_source_files(  # doctest: +SKIP
            ...     session,
            ...     ["gs://bucket/summary_stats/AB1_meta_out.tsv.gz"],
            ...     MetaAnalysisDataSource.FINNGEN_UKBB_MVP,
            ... )
        """
        if not summary_statistics_paths:
            raise ValueError("No summary statistics paths to read.")
        if not session.use_enhanced_bgzip_codec:
            session.logger.warning(
                "use_enhanced_bgzip_codec is False: each summary statistics file is read by a single task."
            )
        groups: dict[tuple[str, ...], list[str]] = {}
        for path in summary_statistics_paths:
            groups.setdefault(tuple(cls.read_header(session, path)), []).append(path)
        session.logger.info(
            f"Reading {len(summary_statistics_paths)} summary statistics files with {len(groups)} distinct headers."
        )
        study_id = f.concat_ws(
            "_",
            f.lit(datasource.value),
            cls.extract_study_phenotype_from_path(f.input_file_name()),
        ).alias("studyId")
        frames = []
        for header, paths in groups.items():
            text = t.StructType([t.StructField(c, t.StringType()) for c in header])
            df = session.spark.read.csv(paths, schema=text, header=True, sep="\t")
            frames.append(
                df.select(
                    study_id,
                    *[
                        (
                            f.when(f.col(c) == "NA", f.lit(None)).otherwise(f.col(c))
                            if c in header
                            else f.lit(None)
                        )
                        .try_cast(cls.raw_schema[c].dataType)
                        .alias(c)
                        for c in cls.source_columns
                    ],
                )
            )
        return reduce(DataFrame.unionByName, frames)

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
            * The input summary statistics hold `studyId` and the `source_columns`, as read by `from_source_files`.
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
            assert min_allele_count_threshold > 0, (
                "Allele count threshold should be positive."
            )
        if perform_min_allele_frequency_filter:
            assert 0.0 <= min_allele_frequency_threshold <= 0.5, (
                "MAF needs to be between 0 and 0.5."
            )

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
            assert imputation_score_threshold >= 0.0, (
                "Imputation score threshold should be positive."
            )
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
