"""deCODE summary statistics datasource module."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session, SummaryStatistics
from gentropy.common.processing import mac, normalize_chromosome
from gentropy.common.stats import pvalue_from_neglogpval
from gentropy.dataset.variant_direction import VariantDirection
from gentropy.datasource.decode import deCODEDataSource


class deCODESummaryStatistics:
    """deCODE summary statistics class."""

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
                f.input_file_name().contains("SMP"),
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
                .repartitionByRange(15, "Chrom", "POS")
                .write.mode("append")
                .partitionBy("studyId")
                .parquet(output_path)
            )

        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            pool.map(
                lambda path: process_one(path, raw_summary_statistics_output_path),
                summary_statistics_list[:2],
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
        """
        d1 = f.abs(eur_af - imp_maf)
        d2 = f.abs((1 - eur_af) - imp_maf)

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
        min_mac_threshold: int,
        min_sample_size_threshold: int,
        flipping_window_size: int,
    ) -> SummaryStatistics:
        """Create deCODESummaryStatistics from raw summary statistics DataFrame.

        Args:
            raw_summary_statistics (DataFrame): Raw summary statistics DataFrame.
            variant_direction (VariantDirection): VariantDirection Dataset.
            min_mac_threshold (int): Minimum minor allele count threshold to filter variants.
            min_sample_size_threshold (int): Minimum sample size threshold to filter variants.
            flipping_window_size (int): Window size for variant flipping logic.

        Returns:
            SummaryStatistics: deCODESummaryStatistics object.

        !!! note "Variant flipping logic"
            The flipping window size has to be the same as the one used for
            creating the variant direction dataset, otherwise the join will produce incorrect results.

        """
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
                (f.col("position") / flipping_window_size)
                .cast(t.LongType())
                .alias("rangeId"),
            )
            # Apply filtering on MAC and sample size
            # This should reduce the number of variants from ~33mln to ~25mln
            .filter(f.col("sampleSize") >= min_sample_size_threshold)
            .filter(mac(f.col("impMAF"), f.col("sampleSize")) >= min_mac_threshold)
            .repartitionByRange(10_000, "chromosome", "rangeId")
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
                f.col("isStrandAmbiguous"),
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
            .repartitionByRange(10_000, "chromosome", "rangeId")
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
            .repartitionByRange(5012 * 10, "studyId", "chromosome", "position")
        )

        return SummaryStatistics(_flipped).sanity_filter()
