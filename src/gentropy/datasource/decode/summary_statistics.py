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

from pydantic import BaseModel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, SummaryStatistics
from gentropy.common.processing import mac, normalize_chromosome
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.decode import deCODEDataSource
from gentropy.datasource.decode.study_index import deCODEStudyIndex


class deCODEHarmonisationConfig(BaseModel):
    """Configuration for deCODE harmonisation step.

    !!! note "Variant flipping logic"
        The flipping window size has to be the same as the one used for
        creating the variant direction dataset, otherwise the join will produce incorrect results.
    """

    min_mac: int
    """Minimal value of Minor Allelic Count to include in harmonisation."""
    min_sample_size: int
    """Minimal value of Sample Size to include in harmoniation."""
    flipping_window_size: int
    """Window size (bp) used to partition the VariantDirection dataset (exact match only!)."""


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
        raw_schema (StructType): Spark schema of the raw deCODE TSV files.
    """

    N_THREAD_OPTIMAL = 10
    N_THREAD_MAX = 500

    raw_schema = t.StructType(
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

    @classmethod
    def txtgz_to_parquet(
        cls,
        session: Session,
        summary_statistics_list: list[str],
        raw_summary_statistics_output_path: str,
        n_threads: int = 500,  # across all pyspark workers
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
                    schema=t.StructType(
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
                    ),
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
            pool.map(
                lambda path: process_one(path, raw_summary_statistics_output_path),
                summary_statistics_list,
            )

    @classmethod
    def _infer_allele_frequency(cls, imp_maf: Column, eur_af: Column) -> Column:
        """Infer allele frequency from imputed MAF and European AF.

        Args:
            imp_maf (Column): Imputed minor allele frequency from source.
            eur_af (Column): European allele frequency from gnomAD.

        Returns:
            Column: Inferred allele frequency.

        !!! note "Inference logic"
            The effect allele frequency (EAF) is inferred by comparing the imputed minor allele frequency (impMAF)
            from deCODE with the European allele frequency (EUR_AF) from gnomAD.

            * If `EUR_AF` is null, the effect allele frequency cannot be inferred. In this case, the imputed
            minor allele frequency (`impMAF`) is used as the EAF. This typically occurs for variants that are
            unique to deCODE and absent from the gnomAD European population.

            * If `EUR_AF` is closer to `impMAF` than to `1 − impMAF`, the effect allele is assumed to be the
            minor allele in the Icelandic population, and `impMAF` is used as the EAF.

            * If `EUR_AF` is closer to `1 − impMAF` than to `impMAF`, the effect allele is assumed to be the
            major allele in the Icelandic population, and `1 − impMAF` is used as the EAF.

        Examples:
            >>> data = [(0.01, 0.02), (0.01, 0.6), (0.01, None)]
            >>> df = spark.createDataFrame(data, ["impMAF", "EUR_AF"])
            >>> df.withColumn("EAF", deCODESummaryStatistics._infer_allele_frequency(f.col("impMAF"), f.col("EUR_AF"))).show()
            +------+------+----+
            |impMAF|EUR_AF| EAF|
            +------+------+----+
            |  0.01|  0.02|0.01|
            |  0.01|   0.6|0.99|
            |  0.01|  NULL|0.01|
            +------+------+----+
            <BLANKLINE>
        """
        d1 = f.abs(eur_af - imp_maf)
        d2 = f.abs(eur_af - (1 - imp_maf))

        return (
            f.when(eur_af.isNull(), imp_maf)
            .when(d1 <= d2, imp_maf)
            .otherwise(1 - imp_maf)
            .alias("effectAlleleFrequencyFromSource")
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

        1. **Schema alignment** – renames deCODE-specific column names to the
           gentropy standard (e.g. ``Chrom`` → ``chromosome``, ``Beta`` → ``beta``).
        2. **MAC / sample-size filtering** – discards variants below
           ``config.min_mac`` or ``config.min_sample_size`` to remove underpowered
           associations and reduce output volume (~33 M → ~25 M rows).
        3. **Allele-flipping** – left-joins against the gnomAD `VariantDirection` dataset
           (positive strand only) using ``(chromosome, rangeId, variantId)``.
           Variants found in gnomAD are flipped to the gnomAD reference orientation;
           unmatched variants are kept as-is.
        4. **EAF inference** – `_infer_allele_frequency` maps the deCODE
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
        # Get the estimated number of distinct studies to set the number of partitions for the final join
        n_sumstats = decode_study_index.df.count()
        pval = pvalue_from_neglogpval(f.col("neglogPval"))
        _sumstats = (
            raw_summary_statistics.select(
                normalize_chromosome(f.col("Chrom")).alias("chromosome"),
                f.col("Pos").cast(t.IntegerType()).alias("position"),
                f.col("effectAllele").alias("alt"),
                f.col("otherAllele").alias("ref"),
                f.col("Beta").cast(t.DoubleType()).alias("beta"),
                f.col("minus_log10_pval").alias("neglogPval"),
                f.col("SE").cast(t.DoubleType()).alias("standardError"),
                f.col("N").cast(t.IntegerType()).alias("sampleSize"),
                f.col("impMAF").cast(t.FloatType()).alias("impMAF"),
                f.col("studyId"),
            )
            .select(
                f.col("studyId"),
                f.concat_ws(
                    "_",
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("alt"),
                    f.col("ref"),
                ).alias("variantId"),
                f.col("chromosome"),
                f.col("position"),
                f.col("beta"),
                f.col("sampleSize"),
                pval.mantissa.alias("pValueMantissa"),
                pval.exponent.alias("pValueExponent"),
                f.col("impMAF"),
                f.col("standardError"),
                f.floor(f.col("position") / config.flipping_window_size)
                .cast(t.IntegerType())
                .alias("rangeId"),
            )
            # Apply filtering on MAC and sample size
            # This should reduce the number of variants from ~33mln to ~25mln
            .filter(f.col("sampleSize") >= config.min_sample_size)
            .filter(mac(f.col("impMAF"), f.col("sampleSize")) >= config.min_mac)
            .repartitionByRange(n_sumstats * 300 * 22, "chromosome", "rangeId", "studyId", "variantId")            
            .persist()
            .alias("sumstats")
        )

        _vd = (
            # Need only positive strand variants
            variant_direction.df.filter(f.col("strand") == 1)
            .select(
                f.col("chromosome"),
                f.col("rangeId"),
                f.col("originalVariantId"),
                f.col("variantId"),
                f.col("direction"),
                f.filter(
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
            .repartitionByRange(n_sumstats * 300 * 22, "chromosome", "rangeId", "variantId")
            .persist()
            .alias("vd")
        )

        
        _flipped = (
            _sumstats.join(_vd, on=["chromosome", "rangeId", "variantId"], how="left")
            .select(
                f.col("sumstats.studyId").alias("studyId"),
                f.coalesce(
                    f.col("vd.originalVariantId"), f.col("sumstats.variantId")
                ).alias("variantId"),
                f.col("sumstats.chromosome").alias("chromosome"),
                f.col("sumstats.position").alias("position"),
                (f.col("sumstats.beta") * f.coalesce(f.col("vd.direction"), f.lit(1)))
                .cast("double")
                .alias("beta"),
                f.col("sumstats.sampleSize").alias("sampleSize"),
                f.col("sumstats.pValueMantissa").alias("pValueMantissa"),
                f.col("sumstats.pValueExponent").alias("pValueExponent"),
                cls._infer_allele_frequency(
                    f.col("sumstats.impMAF"), f.col("vd.eur_af")
                ).alias("effectAlleleFrequencyFromSource"),
                f.col("sumstats.standardError").alias("standardError"),
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
        _vd.unpersist()
        _sumstats.unpersist()

        _harmonised = SummaryStatistics(
            _df=SummaryStatistics(_flipped)
            .sanity_filter()
            .df.join(
                si.select(f.col("updatedStudyId"), f.col("studyId")),
                on="studyId",
                how="left",
            )
            # In case the sumstat was not found in studyIndex, resolve with original studyId
            # To avoid losing summary statistics data.
            .withColumn("studyId", f.coalesce(f.col("updatedStudyId"), f.col("studyId")))
            .drop("updatedStudyId")
            .persist()
        )

        _pqtl_si = ProteinQuantitativeTraitLocusStudyIndex(
            _df=si.drop("studyId").withColumnRenamed("updatedStudyId", "studyId")
            .persist()
        )

        return (_harmonised, _pqtl_si)
