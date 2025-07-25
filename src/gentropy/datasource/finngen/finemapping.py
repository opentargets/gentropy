# pylint: disable=unsubscriptable-object
"""Datasource ingestion: FinnGen Finemapping results (SuSIE) to studyLocus object."""

from __future__ import annotations

from dataclasses import dataclass

import hail as hl
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from gentropy.common.spark import get_top_ranked_in_window
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus


@dataclass
class FinnGenFinemapping:
    """SuSIE finemapping dataset for FinnGen.

    Credible sets from SuSIE are extracted and transformed into StudyLocus objects:

    - Study ID in the special format (e.g. FINNGEN_R11*)
    - Credible set specific finemapping statistics (e.g. LogBayesFactors, Alphas/Posterior)
    - Additional credible set level BayesFactor filtering is applied (LBF > 2)
    - StudyLocusId is annotated for each credible set.

    Finemapping method is populated as a constant ("SuSIE").
    """

    raw_schema: t.StructType = StructType(
        [
            StructField("trait", StringType(), True),
            StructField("region", StringType(), True),
            StructField("v", StringType(), True),
            StructField("rsid", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", StringType(), True),
            StructField("allele1", StringType(), True),
            StructField("allele2", StringType(), True),
            StructField("maf", StringType(), True),
            StructField("beta", StringType(), True),
            StructField("se", StringType(), True),
            StructField("p", StringType(), True),
            StructField("mean", StringType(), True),
            StructField("sd", StringType(), True),
            StructField("prob", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("cs_specific_prob", DoubleType(), True),
            StructField("low_purity", StringType(), True),
            StructField("lead_r2", StringType(), True),
            StructField("mean_99", StringType(), True),
            StructField("sd_99", StringType(), True),
            StructField("prob_99", StringType(), True),
            StructField("cs_99", StringType(), True),
            StructField("cs_specific_prob_99", StringType(), True),
            StructField("low_purity_99", StringType(), True),
            StructField("lead_r2_99", StringType(), True),
            StructField("alpha1", DoubleType(), True),
            StructField("alpha2", DoubleType(), True),
            StructField("alpha3", DoubleType(), True),
            StructField("alpha4", DoubleType(), True),
            StructField("alpha5", DoubleType(), True),
            StructField("alpha6", DoubleType(), True),
            StructField("alpha7", DoubleType(), True),
            StructField("alpha8", DoubleType(), True),
            StructField("alpha9", DoubleType(), True),
            StructField("alpha10", DoubleType(), True),
            StructField("mean1", StringType(), True),
            StructField("mean2", StringType(), True),
            StructField("mean3", StringType(), True),
            StructField("mean4", StringType(), True),
            StructField("mean5", StringType(), True),
            StructField("mean6", StringType(), True),
            StructField("mean7", StringType(), True),
            StructField("mean8", StringType(), True),
            StructField("mean9", StringType(), True),
            StructField("mean10", StringType(), True),
            StructField("sd1", StringType(), True),
            StructField("sd2", StringType(), True),
            StructField("sd3", StringType(), True),
            StructField("sd4", StringType(), True),
            StructField("sd5", StringType(), True),
            StructField("sd6", StringType(), True),
            StructField("sd7", StringType(), True),
            StructField("sd8", StringType(), True),
            StructField("sd9", StringType(), True),
            StructField("sd10", StringType(), True),
            StructField("lbf_variable1", DoubleType(), True),
            StructField("lbf_variable2", DoubleType(), True),
            StructField("lbf_variable3", DoubleType(), True),
            StructField("lbf_variable4", DoubleType(), True),
            StructField("lbf_variable5", DoubleType(), True),
            StructField("lbf_variable6", DoubleType(), True),
            StructField("lbf_variable7", DoubleType(), True),
            StructField("lbf_variable8", DoubleType(), True),
            StructField("lbf_variable9", DoubleType(), True),
            StructField("lbf_variable10", DoubleType(), True),
        ]
    )

    summary_schema: t.StructType = StructType(
        [
            StructField("trait", StringType(), True),
            StructField("region", StringType(), True),
            StructField("cs", StringType(), True),
            StructField("cs_log10bf", DoubleType(), True),
            StructField("cs_avg_r2", DoubleType(), True),
            StructField("cs_min_r2", DoubleType(), True),
        ]
    )

    raw_hail_shema: hl.tstruct = hl.tstruct(
        trait=hl.tstr,
        region=hl.tstr,
        v=hl.tstr,
        rsid=hl.tstr,
        chromosome=hl.tstr,
        position=hl.tstr,
        allele1=hl.tstr,
        allele2=hl.tstr,
        maf=hl.tstr,
        beta=hl.tstr,
        se=hl.tstr,
        p=hl.tstr,
        mean=hl.tstr,
        sd=hl.tstr,
        prob=hl.tstr,
        cs=hl.tstr,
        cs_specific_prob=hl.tfloat64,
        low_purity=hl.tstr,
        lead_r2=hl.tstr,
        mean_99=hl.tstr,
        sd_99=hl.tstr,
        prob_99=hl.tstr,
        cs_99=hl.tstr,
        cs_specific_prob_99=hl.tstr,
        low_purity_99=hl.tstr,
        lead_r2_99=hl.tstr,
        alpha1=hl.tfloat64,
        alpha2=hl.tfloat64,
        alpha3=hl.tfloat64,
        alpha4=hl.tfloat64,
        alpha5=hl.tfloat64,
        alpha6=hl.tfloat64,
        alpha7=hl.tfloat64,
        alpha8=hl.tfloat64,
        alpha9=hl.tfloat64,
        alpha10=hl.tfloat64,
        mean1=hl.tstr,
        mean2=hl.tstr,
        mean3=hl.tstr,
        mean4=hl.tstr,
        mean5=hl.tstr,
        mean6=hl.tstr,
        mean7=hl.tstr,
        mean8=hl.tstr,
        mean9=hl.tstr,
        mean10=hl.tstr,
        sd1=hl.tstr,
        sd2=hl.tstr,
        sd3=hl.tstr,
        sd4=hl.tstr,
        sd5=hl.tstr,
        sd6=hl.tstr,
        sd7=hl.tstr,
        sd8=hl.tstr,
        sd9=hl.tstr,
        sd10=hl.tstr,
        lbf_variable1=hl.tfloat64,
        lbf_variable2=hl.tfloat64,
        lbf_variable3=hl.tfloat64,
        lbf_variable4=hl.tfloat64,
        lbf_variable5=hl.tfloat64,
        lbf_variable6=hl.tfloat64,
        lbf_variable7=hl.tfloat64,
        lbf_variable8=hl.tfloat64,
        lbf_variable9=hl.tfloat64,
        lbf_variable10=hl.tfloat64,
    )

    summary_hail_schema: hl.tstruct = hl.tstruct(
        trait=hl.tstr,
        region=hl.tstr,
        cs=hl.tstr,
        cs_log10bf=hl.tfloat64,
        cs_avg_r2=hl.tfloat64,
        cs_min_r2=hl.tfloat64,
    )

    @staticmethod
    def _infer_block_gzip_compression(paths: str | list[str]) -> bool:
        """Naively infer compression type based on the file extension.

        Args:
            paths (str | list[str]): File path(s).

        Returns:
            bool: True if block gzipped, False otherwise.
        """
        if isinstance(paths, str):
            return paths.endswith(".bgz")
        return all(path.endswith(".bgz") for path in paths)

    @classmethod
    def from_finngen_susie_finemapping(
        cls: type[FinnGenFinemapping],
        spark: SparkSession,
        finngen_susie_finemapping_snp_files: (str | list[str]),
        finngen_susie_finemapping_cs_summary_files: (str | list[str]),
        finngen_release_prefix: str,
        credset_lbf_threshold: float = 0.8685889638065036,
    ) -> StudyLocus:
        """Process the SuSIE finemapping output for FinnGen studies.

        The finngen_susie_finemapping_snp_files are files that contain variant summaries with credible set information with following shema:
            - trait: phenotype
            - region: region for which the fine-mapping was run.
            - v, rsid: variant ids
            - chromosome
            - position
            - allele1
            - allele2
            - maf: minor allele frequency
            - beta: original marginal beta
            - se: original se
            - p: original p
            - mean: posterior mean beta after fine-mapping
            - sd: posterior standard deviation after fine-mapping.
            - prob: posterior inclusion probability
            - cs: credible set index within region
            - lead_r2: r2 value to a lead variant (the one with maximum PIP) in a credible set
            - alphax: posterior inclusion probability for the x-th single effect (x := 1..L where L is the number of single effects (causal variants) specified; default: L = 10).
            - lbfx: log-Bayes Factor for each variable and single effect (i.e credible set).
            - meanx: posterior mean for each variable and single effect (i.e credible set).
            - sdx: posterior sd of mean for each variable and single effect (i.e credible set).
        As for r11 finngen release these files are ingested from `https://console.cloud.google.com/storage/browser/finngen-public-data-r11/finemap/full/susie/` by
            - *.snp.bgz
            - *.snp.bgz.tbi
        Each file contains index (.tbi) file that is required to read the block gzipped compressed snp file. These files needs to be
        downloaded, transfromed from block gzipped to plain gzipped and then uploaded to the storage bucket, before they can be read by spark or read by hail directly as import table.

        The finngen_susie_finemapping_cs_summary_files are files that Contains credible set summaries from SuSiE fine-mapping for all genome-wide significant regions with following schema:
            - trait: phenotype
            - region: region for which the fine-mapping was run.
            - cs: running number for independent credible sets in a region, assigned to 95% PIP
            - cs_log10bf: Log10 bayes factor of comparing the solution of this model (cs independent credible sets) to cs -1 credible sets
            - cs_avg_r2: Average correlation R2 between variants in the credible set
            - cs_min_r2: minimum r2 between variants in the credible set
            - low_purity: boolean (TRUE,FALSE) indicator if the CS is low purity (low min r2)
            - cs_size: how many snps does this credible set contain
            - good_cs: boolean (TRUE,FALSE) indicator if this CS is considered reliable. IF this is FALSE then top variant reported for the CS will be chosen based on minimum p-value in the credible set, otherwise top variant is chosen by maximum PIP
            - cs_id:
            - v: top variant (chr:pos:ref:alt)
            - p: top variant p-value
            - beta: top variant beta
            - sd: top variant standard deviation
            - prob: overall PIP of the variant in the region
            - cs_specific_prob: PIP of the variant in the current credible set (this and previous are typically almost identical)
            - 0..n: configured annotation columns. Typical default most_severe,gene_most_severe giving consequence and gene of top variant
        These files needs to be downloaded from the `https://console.cloud.google.com/storage/browser/finngen-public-data-r11/finemap/summary/` by *.cred.summary.tsv pattern,

        Args:
            spark (SparkSession): SparkSession object.
            finngen_susie_finemapping_snp_files (str | list[str]): SuSIE finemapping output filename(s).
            finngen_susie_finemapping_cs_summary_files (str | list[str]): filename of SuSIE finemapping credible set summaries.
            finngen_release_prefix (str): Finngen project release prefix. Should look like FINNGEN_R*.
            credset_lbf_threshold (float, optional): Filter out credible sets below, Default 0.8685889638065036 == np.log10(np.exp(2)), this is threshold from publication.

        Returns:
            StudyLocus: Processed SuSIE finemapping output in StudyLocus format.
        """
        # NOTE: hail allows for importing block gzipped files, spark does not without external libraries.
        # check https://github.com/projectglow/glow/blob/36bf6121fbc4ccc33a13b028deb87b63faeba7a9/core/src/main/scala/io/projectglow/vcf/VCFFileFormat.scala#L274
        # how it could be implemented with spark.
        bgzip_compressed_snps = cls._infer_block_gzip_compression(
            finngen_susie_finemapping_snp_files
        )

        # NOTE: fallback to spark read if not block gzipped file in the input
        if bgzip_compressed_snps:
            snps_df = hl.import_table(
                finngen_susie_finemapping_snp_files,
                delimiter="\t",
                types=cls.raw_hail_shema,
            ).to_spark()
        else:
            snps_df = (
                spark.read.schema(cls.raw_schema)
                .option("delimiter", "\t")
                .option("compression", "gzip")
                .csv(finngen_susie_finemapping_snp_files, header=True)
            )

        processed_finngen_finemapping_df = (
            # Drop rows which don't have proper position.
            snps_df.filter(f.col("position").cast(t.IntegerType()).isNotNull())
            # Drop non credible set SNPs:
            .filter(f.col("cs").cast(t.IntegerType()) > 0)
            .select(
                # Add study idenfitier.
                f.concat_ws("_", f.lit(finngen_release_prefix), f.col("trait"))
                .cast(t.StringType())
                .alias("studyId"),
                f.col("region"),
                # Add variant information.
                f.regexp_replace(f.col("v"), ":", "_").alias("variantId"),
                f.col("cs").cast("integer").alias("credibleSetIndex"),
                f.regexp_replace(f.col("chromosome"), "^chr", "")
                .cast(t.StringType())
                .alias("chromosome"),
                f.col("position").cast(t.IntegerType()),
                f.col("allele1").cast(t.StringType()).alias("ref"),
                f.col("allele2").cast(t.StringType()).alias("alt"),
                # Parse p-value into mantissa and exponent.
                *split_pvalue_column(f.col("p")),
                # Add standard error, and allele frequency information.
                f.col("se").cast("double").alias("standardError"),
                f.col("maf").cast("float").alias("effectAlleleFrequencyFromSource"),
                f.lit(FinemappingMethod.SUSIE.value).alias("finemappingMethod"),
                *[
                    f.col(f"alpha{i}").cast(t.DoubleType()).alias(f"alpha_{i}")
                    for i in range(1, 11)
                ],
                *[
                    f.col(f"lbf_variable{i}").cast(t.DoubleType()).alias(f"lbf_{i}")
                    for i in range(1, 11)
                ],
                *[
                    f.col(f"mean{i}").cast(t.DoubleType()).alias(f"beta_{i}")
                    for i in range(1, 11)
                ],
            )
            .withColumn(
                "posteriorProbability",
                f.when(f.col("credibleSetIndex") == 1, f.col("alpha_1"))
                .when(f.col("credibleSetIndex") == 2, f.col("alpha_2"))
                .when(f.col("credibleSetIndex") == 3, f.col("alpha_3"))
                .when(f.col("credibleSetIndex") == 4, f.col("alpha_4"))
                .when(f.col("credibleSetIndex") == 5, f.col("alpha_5"))
                .when(f.col("credibleSetIndex") == 6, f.col("alpha_6"))
                .when(f.col("credibleSetIndex") == 7, f.col("alpha_7"))
                .when(f.col("credibleSetIndex") == 8, f.col("alpha_8"))
                .when(f.col("credibleSetIndex") == 9, f.col("alpha_9"))
                .when(f.col("credibleSetIndex") == 10, f.col("alpha_10")),
            )
            .drop(
                "alpha_1",
                "alpha_2",
                "alpha_3",
                "alpha_4",
                "alpha_5",
                "alpha_6",
                "alpha_7",
                "alpha_8",
                "alpha_9",
                "alpha_10",
            )
            .withColumn(
                "logBF",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_1"))
                .when(f.col("credibleSetIndex") == 2, f.col("lbf_2"))
                .when(f.col("credibleSetIndex") == 3, f.col("lbf_3"))
                .when(f.col("credibleSetIndex") == 4, f.col("lbf_4"))
                .when(f.col("credibleSetIndex") == 5, f.col("lbf_5"))
                .when(f.col("credibleSetIndex") == 6, f.col("lbf_6"))
                .when(f.col("credibleSetIndex") == 7, f.col("lbf_7"))
                .when(f.col("credibleSetIndex") == 8, f.col("lbf_8"))
                .when(f.col("credibleSetIndex") == 9, f.col("lbf_9"))
                .when(f.col("credibleSetIndex") == 10, f.col("lbf_10")),
            )
            .drop(
                "lbf_1",
                "lbf_2",
                "lbf_3",
                "lbf_4",
                "lbf_5",
                "lbf_6",
                "lbf_7",
                "lbf_8",
                "lbf_9",
                "lbf_10",
            )
            .withColumn(
                "beta",
                f.when(f.col("credibleSetIndex") == 1, f.col("beta_1"))
                .when(f.col("credibleSetIndex") == 2, f.col("beta_2"))
                .when(f.col("credibleSetIndex") == 3, f.col("beta_3"))
                .when(f.col("credibleSetIndex") == 4, f.col("beta_4"))
                .when(f.col("credibleSetIndex") == 5, f.col("beta_5"))
                .when(f.col("credibleSetIndex") == 6, f.col("beta_6"))
                .when(f.col("credibleSetIndex") == 7, f.col("beta_7"))
                .when(f.col("credibleSetIndex") == 8, f.col("beta_8"))
                .when(f.col("credibleSetIndex") == 9, f.col("beta_9"))
                .when(f.col("credibleSetIndex") == 10, f.col("beta_10")),
            )
            .drop(
                "beta_1",
                "beta_2",
                "beta_3",
                "beta_4",
                "beta_5",
                "beta_6",
                "beta_7",
                "beta_8",
                "beta_9",
                "beta_10",
            )
        )

        bgzip_compressed_cs_summaries = cls._infer_block_gzip_compression(
            finngen_susie_finemapping_cs_summary_files
        )

        # NOTE: fallback to spark read if not block gzipped file in the input
        # in case we want to use the raw files from the
        # https://console.cloud.google.com/storage/browser/finngen-public-data-r11/finemap/full/susie/*.cred.gz
        if bgzip_compressed_cs_summaries:
            cs_summary_df = hl.import_table(
                finngen_susie_finemapping_cs_summary_files,
                delimiter="\t",
                types=cls.summary_hail_schema,
            ).to_spark()
        else:
            cs_summary_df = (
                spark.read.schema(cls.summary_schema)
                .option("delimiter", "\t")
                .csv(finngen_susie_finemapping_cs_summary_files, header=True)
            )

        # drop credible sets where logbf > 2. Except when there's only one credible set in region:
        # 0.8685889638065036 corresponds to np.log10(np.exp(2)), to match the orginal threshold in publication.
        finngen_finemapping_summaries_df = (
            # Read credible set level lbf, it is output as a different file which is not ideal.
            cs_summary_df.select(
                f.col("region"),
                f.col("trait"),
                f.col("cs").cast("integer").alias("credibleSetIndex"),
                f.col("cs_log10bf").cast("double").alias("credibleSetlog10BF"),
                f.col("cs_avg_r2").cast("double").alias("purityMeanR2"),
                f.col("cs_min_r2").cast("double").alias("purityMinR2"),
            )
            .filter(
                (f.col("credibleSetlog10BF") > credset_lbf_threshold)
                | (f.col("credibleSetIndex") == 1)
            )
            .withColumn(
                "studyId",
                f.concat_ws("_", f.lit(finngen_release_prefix), f.col("trait")),
            )
        )

        processed_finngen_finemapping_df = processed_finngen_finemapping_df.join(
            finngen_finemapping_summaries_df,
            on=["studyId", "region", "credibleSetIndex"],
            how="inner",
        )

        toploci_df = get_top_ranked_in_window(
            processed_finngen_finemapping_df,
            Window.partitionBy("studyId", "region", "credibleSetIndex").orderBy(
                f.desc("posteriorProbability")
            ),
        ).select(
            "variantId",
            "chromosome",
            "position",
            "studyId",
            "beta",
            "pValueMantissa",
            "pValueExponent",
            "effectAlleleFrequencyFromSource",
            "standardError",
            "region",
            "credibleSetIndex",
            "finemappingMethod",
            "credibleSetlog10BF",
            "purityMeanR2",
            "purityMinR2",
        )

        processed_finngen_finemapping_df = (
            processed_finngen_finemapping_df.groupBy(
                "studyId", "region", "credibleSetIndex"
            )
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("variantId").cast("string").alias("variantId"),
                        f.col("posteriorProbability")
                        .cast("double")
                        .alias("posteriorProbability"),
                        f.col("logBF").cast("double").alias("logBF"),
                        f.col("pValueMantissa").cast("float").alias("pValueMantissa"),
                        f.col("pValueExponent").cast("integer").alias("pValueExponent"),
                        f.col("beta").cast("double").alias("beta"),
                        f.col("standardError").cast("double").alias("standardError"),
                    )
                ).alias("locus"),
            )
            .select(
                "studyId",
                "region",
                "credibleSetIndex",
                "locus",
            )
            .join(
                toploci_df,
                on=["studyId", "region", "credibleSetIndex"],
                how="inner",
            )
            .withColumns(
                {
                    "locusStart": f.split(f.split("region", ":")[1], "-")[0].cast(
                        "int"
                    ),
                    "locusEnd": f.split(f.split("region", ":")[1], "-")[1].cast("int"),
                }
            )
        ).withColumn(
            "studyLocusId",
            StudyLocus.assign_study_locus_id(
                ["studyId", "variantId", "finemappingMethod"]
            ),
        )

        return StudyLocus(
            _df=processed_finngen_finemapping_df,
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()
