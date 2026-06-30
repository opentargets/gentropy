"""deCODE summary statistics module.

This module provides:

- **`deCODEHarmonisationConfig`** – a Pydantic model that holds the quality-control
  and harmonisation parameters (MAC threshold, sample-size threshold, flipping window).

- **`deCODESummaryStatistics`** – a utility class with two main pipelines:

  1. `txtgz_to_parquet` – parallel ingestion of raw gzipped TSV files from the
     deCODE S3 bucket into partitioned Parquet.
  2. `from_source` – harmonisation (schema alignment, MAC/sample-size filtering,
     allele flipping against gnomAD EUR AF, EAF inference, sanity filtering, and
     study-ID update).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pydantic import BaseModel, ConfigDict, Field
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, SummaryStatistics
from gentropy.common.processing import (
    flag_equal_alleles,
    flag_non_atgc_alleles,
    infer_allele_frequency_from_maf,
    mac,
    normalize_chromosome,
)
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.dataset.variant_direction import DEFAULT_WINDOW_SIZE, VariantDirection
from gentropy.datasource.decode import deCODEDataSource
from gentropy.datasource.decode.study_index import deCODEStudyIndex

DECODE_SCHEMA = t.StructType(
    [
        t.StructField("Chrom", t.StringType()),
        t.StructField("Pos", t.LongType()),
        t.StructField("Name", t.StringType()),
        t.StructField("rsids", t.StringType()),
        t.StructField("effectAllele", t.StringType()),
        t.StructField("otherAllele", t.StringType()),
        t.StructField("Beta", t.DoubleType()),
        t.StructField("Pval", t.DoubleType()),
        t.StructField("minus_log10_pval", t.DoubleType()),
        t.StructField("SE", t.DoubleType()),
        t.StructField("N", t.LongType()),
        t.StructField("impMAF", t.DoubleType()),
    ]
)


class deCODEHarmonisationConfig(BaseModel):
    """Configuration for deCODE harmonisation step.

    !!! note "Variant flipping logic"
        The flipping window size has to be the same as the one used for
        creating the variant direction dataset, otherwise the join will produce incorrect results.
        The default value is sourced from `gentropy.dataset.variant_direction.DEFAULT_WINDOW_SIZE`
        to keep both sides in sync.
    """

    perform_min_allele_count_filter: bool
    """Whether to filter variants based on minor allele count (MAC) threshold."""
    min_allele_count_threshold: int = Field(ge=1)
    """Minimum minor allele count required to retain a variant."""

    perform_samples_size_filter: bool
    """Whether to remove variants with low sample size."""
    sample_size_threshold: int = Field(ge=1)
    """Minimum sample size to retain a variant. Must be >= 1."""

    flipping_window_size: int = DEFAULT_WINDOW_SIZE
    """Window size (bp) used to partition the VariantDirection dataset (exact match only!).
        Defaults to `DEFAULT_WINDOW_SIZE` from `gentropy.dataset.variant_direction`.
    """
    remove_monomorphic_alleles: bool
    """Whether to remove variants with equal effect and other alleles during harmonisation."""
    remove_ambiguous_alleles: bool
    """Whether to exclude strand-ambiguous variants (A/T or C/G) from the gnomAD
    reference slice used for allele flipping.

    When ``True``, ambiguous entries are dropped from ``vd_slice`` so they cannot
    influence the flip direction. Ambiguous variants in the summary statistics that
    have **no gnomAD match** are still retained in the output with direction=1
    (no flip) — they are not removed from the harmonised result. This is intentional:
    excluding unmatched ambiguous variants would discard data without any evidence
    that the orientation is wrong.
    """
    verify_atgc: bool
    """Whether to verify that all alleles are A/T/G/C during harmonisation.
        Strict ATGC validation also removes `*` (star) and `!` (multiallelic) alleles,
        so no dedicated symbol filters are required.
    """

    model_config = ConfigDict(extra="forbid")


class deCODESummaryStatistics:
    """Utility class for ingesting and harmonising deCODE proteomics summary statistics.

    This class is never instantiated directly. It exposes two class-method pipelines:

    * `txtgz_to_parquet` – reads one or more gzipped TSV files from the deCODE
      S3 bucket in parallel (using a `ThreadPoolExecutor`)
      and writes them as Parquet files partitioned by ``studyId``.

    * `from_source` – takes the raw Parquet output together with the
      `VariantDirection` gnomAD reference
      and the `ProteinQuantitativeTraitLocusStudyIndex`
      and produces fully harmonised `SummaryStatistics` and an updated
      study index with curated study IDs.

    Class attributes:
        N_THREAD_OPTIMAL (int): Recommended number of ingestion threads (10).
        N_THREAD_MAX (int): Hard upper limit on ingestion threads (500).
    """

    N_THREAD_OPTIMAL = 10
    N_THREAD_MAX = 500

    @classmethod
    def txtgz_to_parquet(
        cls,
        session: Session,
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        n_threads: int = N_THREAD_OPTIMAL,
    ) -> None:
        """Convert txt.gz (tsv) summary statistics to Parquet format.

        This method reads multiple gzipped TSV summary statistics files,
        processes them in parallel using the specified number of threads,
        and writes the combined output in Parquet format, partitioned by studyId.

        Args:
            session (Session): Gentropy session.
            summary_statistics_list (list[str]): List of summary statistics paths.
            raw_summary_statistics_output_path (str): Output path for raw summary statistics in Parquet format.
            n_threads (int): Number of threads to use.

        """
        if len(summary_statistics_list) == 0:
            session.logger.warning("No summary statistics paths found to process.")
            return

        if not isinstance(n_threads, int) or n_threads < 1:
            session.logger.warning(
                f"Invalid n_threads value: {n_threads}. Falling back to 10 threads."
            )
            n_threads = cls.N_THREAD_OPTIMAL
        if n_threads < cls.N_THREAD_OPTIMAL:
            session.logger.warning(
                f"Using low n_threads value: {n_threads}. This may lead to sub-optimal performance."
            )
        if n_threads > cls.N_THREAD_MAX:
            session.logger.warning(
                f"Using high n_threads value: {n_threads}, this may lead to overloading spark driver."
            )
            n_threads = cls.N_THREAD_MAX

        def process_one(input_path: str, output_path: str) -> None:
            session.logger.info(
                f"Converting gzipped summary statistics to Parquet from {input_path} to {output_path}."
            )
            project_id = f.when(
                f.input_file_name().contains("Proteomics_SMP"),
                f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
            ).otherwise(deCODEDataSource.DECODE_PROTEOMICS_RAW.value)
            (
                session.spark.read.csv(
                    input_path,
                    sep="\t",
                    header=True,
                    schema=DECODE_SCHEMA,
                )
                .withColumn(
                    "studyId",
                    f.concat_ws(
                        "_",
                        project_id,
                        f.regexp_extract(
                            f.input_file_name(), r"^.*/(Proteomics_.*)\.txt.gz$", 1
                        ),
                    ),
                )
                # Ensure that the size of each partition is ~100Mb
                .repartitionByRange(15, "Chrom", "Pos")
                .write.mode("append")
                .partitionBy("studyId")
                .parquet(output_path)
            )

        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            list(
                pool.map(
                    lambda path: process_one(path, raw_summary_statistics_output_path),
                    summary_statistics_list,
                )
            )

    @classmethod
    def from_source(
        cls,
        raw_summary_statistics: DataFrame,
        variant_direction: VariantDirection,
        decode_study_index: ProteinQuantitativeTraitLocusStudyIndex,
        config: deCODEHarmonisationConfig,
    ) -> tuple[SummaryStatistics, ProteinQuantitativeTraitLocusStudyIndex]:
        """Harmonise raw deCODE summary statistics and produce an updated study index.

        The harmonisation pipeline performs the following steps in order:

        1. **Schema alignment** – renames deCODE-specific column names and builds
           the source-oriented variant ID using ``chr_pos_ref_alt``.
        2. **Allele, MAC, and sample-size filtering** – applies the enabled
           validity filters and configurable thresholds. If
           ``config.remove_ambiguous_alleles`` is ``True``, strand-ambiguous
           variants (A/T or C/G) are removed from the gnomAD reference slice
           only — ambiguous sumstat variants absent from gnomAD are **retained**
           with no flip applied (see `deCODEHarmonisationConfig.remove_ambiguous_alleles`).
        3. **Allele-flipping** – left-joins against the gnomAD `VariantDirection` dataset
           (positive strand only) using ``(chromosome, rangeId, variantId)``.
           Variants found in gnomAD are flipped to the gnomAD reference orientation;
           unmatched variants are kept as-is.
        4. **EAF inference** – `infer_allele_frequency_from_maf` maps the deCODE
           ``impMAF`` to an effect-allele frequency using the gnomAD EUR AF.
        5. **Sanity filter** – applies the standard `SummaryStatistics.sanity_filter`.
        6. **Study-ID update** – replaces the raw study IDs derived from file paths
           with IDs that embed curated gene symbols and protein names from the
           aptamer mapping table.

        !!! note "Variant flipping window"
            ``config.flipping_window_size`` **must** match the window used when
            building the `VariantDirection` dataset. A mismatch will silently produce
            incorrect join keys.

        Args:
            raw_summary_statistics (DataFrame): Raw summary-statistics Parquet DataFrame
                as produced by `txtgz_to_parquet`.
            variant_direction (VariantDirection): gnomAD variant-direction reference
                used for allele flipping and EAF inference.
            decode_study_index (ProteinQuantitativeTraitLocusStudyIndex): pQTL study
                index produced by `deCODEStudyIndex.from_manifest`.
            config (deCODEHarmonisationConfig): Configuration used during the harmonisation step.

        Returns:
            tuple[SummaryStatistics, ProteinQuantitativeTraitLocusStudyIndex]:
                A 2-tuple of the harmonised summary statistics and the study index
                with updated, curated study IDs.
        """
        vd_slice = variant_direction.df

        if config.remove_ambiguous_alleles:
            vd_slice = vd_slice.filter(~f.col("isStrandAmbiguous"))

        vd_slice = (
            # deCODE alleles are compared only with positive-strand reference entries.
            vd_slice.filter(f.col("strand") == 1)
            .select(
                f.col("chromosome"),
                f.col("rangeId"),
                f.col("originalVariantId"),
                f.col("variantId"),
                f.col("direction"),
                f.filter(
                    # NFE is the closest population-specific gnomAD frequency available.
                    # TODO: consider using icelandic EAF https://www.ebi.ac.uk/ena/browser/view/PRJEB15197
                    f.col("originalAlleleFrequencies"),
                    lambda x: x.getField("populationName") == "nfe_adj",
                )
                .getItem(0)
                .getField("alleleFrequency")
                .cast(t.FloatType())
                .alias("eur_af"),
            )
            # NOTE: repartition("chromosome") produces very uneven partitions,
            # Spark attempts then to fall back to `dynamic partitioning` algorithm
            # which fails after N failures.
            .persist()
            .alias("vd")
        )

        # Estimate output partitions from the number of studies.
        n_sumstats = decode_study_index.df.count()
        # Pre-filtering on alleles based on configuration.
        sumstats = raw_summary_statistics
        if config.remove_monomorphic_alleles:
            sumstats = sumstats.filter(
                flag_equal_alleles(f.col("effectAllele"), f.col("otherAllele"))
            )
        if config.verify_atgc:
            sumstats = sumstats.filter(
                flag_non_atgc_alleles(f.col("effectAllele"), f.col("otherAllele"))
            )

        sumstats = (
            sumstats.select(
                f.col("studyId"),
                normalize_chromosome(f.col("Chrom")).alias("chromosome"),
                f.col("Pos").cast(t.IntegerType()).alias("position"),
                f.col("effectAllele").alias("alternateAllele"),
                f.col("otherAllele").alias("referenceAllele"),
                f.col("Beta").alias("beta"),
                f.col("SE").alias("standardError"),
                f.col("minus_log10_pval").alias("neglogPval"),
                f.col("N").cast(t.IntegerType()).alias("sampleSize"),
                f.col("impMAF").cast(t.FloatType()).alias("minorAlleleFrequency"),
                f.floor(f.col("position") / config.flipping_window_size)
                .cast(t.IntegerType())
                .alias("rangeId"),
            )
            .filter(f.col("neglogPval").isNotNull())
            .filter(f.col("beta").isNotNull())
            .filter(f.col("standardError").isNotNull())
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
        )

        if config.perform_samples_size_filter:
            sumstats = sumstats.filter(
                f.col("sampleSize") >= config.sample_size_threshold
            )
        if config.perform_min_allele_count_filter:
            sumstats = sumstats.filter(
                mac(f.col("minorAlleleFrequency"), f.col("sampleSize"))
                >= config.min_allele_count_threshold
            )

        flipped = (
            sumstats.join(
                vd_slice, on=["chromosome", "rangeId", "variantId"], how="left"
            )
            .select(
                f.col("studyId").alias("studyId"),
                f.coalesce(f.col("originalVariantId"), f.col("variantId")).alias(
                    "variantId"
                ),
                f.col("chromosome").alias("chromosome"),
                f.col("position").alias("position"),
                (f.col("beta") * f.coalesce(f.col("direction"), f.lit(1))).alias(
                    "beta"
                ),
                f.col("sampleSize").alias("sampleSize"),
                f.col("neglogPval").alias("neglogPval"),
                infer_allele_frequency_from_maf(
                    f.col("minorAlleleFrequency"), f.col("eur_af")
                ).alias("effectAlleleFrequencyFromSource"),
                f.col("standardError").alias("standardError"),
            )
            .select(
                f.col("studyId"),
                f.col("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.col("sampleSize"),
                *pvalue_from_neglogpval(f.col("neglogPval")),
                f.col("effectAlleleFrequencyFromSource"),
                f.col("standardError"),
            )
            .sort("studyId", "chromosome", "position")
            # Approximate number of partitions = 15 * number of studies
            .repartitionByRange(n_sumstats * 10, "studyId", "chromosome", "position")
        )

        si = decode_study_index.df.withColumn(
            "updatedStudyId",
            deCODEStudyIndex.update_study_id(
                f.col("studyId"), f.col("targetsFromSource")
            ),
        )
        harmonised = SummaryStatistics(
            _df=SummaryStatistics(flipped)
            .sanity_filter()
            .df.join(
                si.select(f.col("updatedStudyId"), f.col("studyId")),
                on="studyId",
                how="left",
            )
            # In case the sumstat was not found in studyIndex, resolve with original studyId
            # To avoid losing summary statistics data.
            .withColumn(
                "studyId", f.coalesce(f.col("updatedStudyId"), f.col("studyId"))
            )
            .drop("updatedStudyId")
            .persist()
        )
        # vd_slice is no longer needed once harmonised is registered for caching.
        vd_slice.unpersist()

        pqtl_si = ProteinQuantitativeTraitLocusStudyIndex(
            _df=si.drop("studyId")
            .withColumnRenamed("updatedStudyId", "studyId")
            .persist()
        )

        return (harmonised, pqtl_si)
