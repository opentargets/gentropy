"""Process SuSIE finemapping results from eQTL Catalogue."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gentropy.common.processing import flag_non_atgc_alleles, normalize_chromosome
from gentropy.common.session import NativeFileFormat, Session
from gentropy.common.spark import clean_strings_from_symbols
from gentropy.common.stats import (
    region_log_bayes_factor,
    split_pvalue_column,
)
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_locus import FinemappingMethod
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex

if TYPE_CHECKING:
    pass


@dataclass
class EqtlCatalogueFinemapping:
    """SuSIE finemapping dataset for eQTL Catalogue.

    Credible sets from SuSIE are extracted and transformed into CredibleSet and CredibleSetVariant objects:
    - A study ID is defined as a triad between: the publication, the tissue, and the measured trait (e.g. Braineac2_substantia_nigra_ENSG00000248275)
    - Each row in the `*.credible_set.parquet` files is represented by molecular_trait_id/variant/rsid trios relevant for a given tissue. Each have their own finemapping statistics
    - log Bayes Factors are available for all variants in the `*.lbf_variable.parquet` files
    - the SuSiE posterior of every effect is available for all variants in the `*.full_susie.parquet` files
    """

    raw_credible_set_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", IntegerType(), True),
            StructField("ref", StringType(), True),
            StructField("alt", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("ma_samples", IntegerType(), True),
            StructField("maf", DoubleType(), True),
            StructField("pvalue", DoubleType(), True),
            StructField("beta", DoubleType(), True),
            StructField("se", DoubleType(), True),
            StructField("type", StringType(), True),
            StructField("ac", IntegerType(), True),
            StructField("r2", StringType(), True),
            StructField("molecular_trait_object_id", StringType(), True),
            StructField("gene_id", StringType(), True),
            StructField("median_tmp", DoubleType(), True),
            StructField("rsid", StringType(), True),
            StructField("cs_id", StringType(), True),
            StructField("cs_size", IntegerType(), True),
            StructField("pip", DoubleType(), True),
            StructField("z", DoubleType(), True),
            StructField("cs_min_r2", DoubleType(), True),
            StructField("region", StringType(), True),
        ]
    )
    raw_lbf_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("chromosome", StringType(), True),
            StructField("position", IntegerType(), True),
            *[
                StructField(f"lbf_variable{i}", DoubleType(), True)
                for i in range(1, 11)
            ],
        ]
    )

    raw_full_susie_schema: StructType = StructType(
        [
            StructField("molecular_trait_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("variant", StringType(), True),
            StructField("cs_index", StringType(), True),
            *[StructField(f"mu_{i}", DoubleType(), True) for i in range(1, 11)],
            *[StructField(f"mu2_{i}", DoubleType(), True) for i in range(1, 11)],
        ]
    )

    @classmethod
    def scale_pip(
        cls: type[EqtlCatalogueFinemapping],
        df: DataFrame,
        tolerance: float = 0.00001,
    ) -> DataFrame:
        r"""Scales PIP when PIPsum is above 1.0 + tolerance.

        This computation ensures that PIPsum is in the range $`[0, 1.0]`$. The origin of this bug
        in the data comes from incorrect assignment of PIP value computed in SuSiE results per variant instead of
        per (credible-set x variant) alpha value assignment. The actual credible-set PIP values (alpha) can be
        rescued from lbf variables for the $`l`$-th component.

        Assumes a flat prior and the LBF in named $`L_1,\ldots,L_l`$ columns.

        For variant $`j`$, $`\alpha_j`$ is defined as:

        ```math
        \alpha_j =
        \frac{
            \exp\left(\operatorname{LBF}_j\right)
        }{
            \sum_{k \in V}
            \exp\left(\operatorname{LBF}_k\right)
        }
        ```

        For numerical stability, the denominator is computed using log-sum-exp.
        First, define the maximum log Bayes factor:

        ```math
        m = \max_{k \in V}\operatorname{LBF}_k
        ```

        The log-sum-exp is then:

        ```math
        \operatorname{LSE}
        =
        m
        +
        \log\left(
            \sum_{k \in V}
            \exp\left(\operatorname{LBF}_k-m\right)
        \right)
        ```

        Finally, alpha is computed as:

        ```math
        \alpha_j
        =
        \exp\left(
            \operatorname{LBF}_j-\operatorname{LSE}
        \right)
        ```

        The resulting alpha values satisfy:

        ```math
        0 \leq \alpha_j \leq 1,
        \qquad
        \sum_{j \in V}\alpha_j = 1
        ```

        This function does following:

        1. Compute PIPsum over the $`l`$-th credible set using the reported `pip` column.
        2. When PIPsum is above the cutoff (1.0 + tolerance), mark the whole study as having per-variant PIP in the column instead of a per-variant, per-credible-set alpha value.*
        3. Choose the `lbf_variable` that represents the $`l`$-th credible-set component (e.g. $`\mathtt{credibleSetIndex}=1 \rightarrow l=1 \rightarrow \mathtt{lbf\_variable1}`$).
        4. Find $`\max(\operatorname{lbf}_l)`$ over all variants in the $`l`$-th credible-set component.
        5. Compute $`\operatorname{logsumexp}(\operatorname{lbf}_l)`$ over all variants in the $`l`$-th credible-set component.
        6. Compute alpha by exponentiating the difference between the $`j`$-th variant's log Bayes factor in the $`l`$-th component, $`\operatorname{lbf}_{jl}`$, and $`\operatorname{logsumexp}(\operatorname{lbf}_l)`$.

        *The whole study is misaligned because all credible sets come from the same input file and therefore share the same problem with PIP values, even when PIPsum is not above $`1.0+\mathtt{tolerance}`$ for some credible sets.

        Args:
            df (DataFrame): DataFrame containing the log Bayes factor columns.
            tolerance (float): Delta tolerance added to 1.0 to reflect numerical precision issues.

        Examples:
            `study1` has two credible sets (`cs1`, `cs2`). `cs1`'s reported `pip` sums to `1.2`,
            above the cutoff, so the whole study is treated as carrying per-variant PIP and `cs2`
            is corrected too, even though its own sum (`0.6`) was within tolerance. `study2`'s
            `cs3` sums to `0.4`, so it is never flagged and its `pip` values are left untouched.

            >>> data = [
            ...     ("study1", "d1", "r1", "g1", "cs1", "v1", 0.6, 2.0),
            ...     ("study1", "d1", "r1", "g1", "cs1", "v2", 0.6, 1.0),
            ...     ("study1", "d1", "r1", "g1", "cs2", "v3", 0.3, 1.5),
            ...     ("study1", "d1", "r1", "g1", "cs2", "v4", 0.3, 0.5),
            ...     ("study2", "d2", "r2", "g2", "cs3", "v5", 0.2, 1.0),
            ...     ("study2", "d2", "r2", "g2", "cs3", "v6", 0.2, 0.0),
            ... ]
            >>> columns = ["study_id", "dataset_id", "region", "gene_id", "cs_id", "variant", "pip", "logBF"]
            >>> df = spark.createDataFrame(data, columns)
            >>> EqtlCatalogueFinemapping.scale_pip(df).select(
            ...     "study_id", "cs_id", "variant", f.round("pip", 4).alias("pip")
            ... ).orderBy("study_id", "cs_id", "variant").show()
            +--------+-----+-------+------+
            |study_id|cs_id|variant|   pip|
            +--------+-----+-------+------+
            |  study1|  cs1|     v1|0.7311|
            |  study1|  cs1|     v2|0.2689|
            |  study1|  cs2|     v3|0.7311|
            |  study1|  cs2|     v4|0.2689|
            |  study2|  cs3|     v5|   0.2|
            |  study2|  cs3|     v6|   0.2|
            +--------+-----+-------+------+
            <BLANKLINE>

        Returns:
            DataFrame: DataFrame where `pip` is replaced with recomputed per-credible-set alpha for affected studies.
        """
        # First we compute the boolean condition checking if PIP sum over single credible
        # set variants is above 1.0 + tolerance
        cs = Window.partitionBy("cs_id", "gene_id", "region", "dataset_id", "study_id")
        study = Window.partitionBy("study_id")

        lbf_jl = f.col("logBF")
        max_lbf_l = f.max(lbf_jl).over(cs)
        sum_lbf_l = max_lbf_l + f.log(f.sum(f.exp(lbf_jl - max_lbf_l)).over(cs))

        return (
            df.withColumn(
                "pipSumAboveCutoff",
                f.sum("pip").over(cs) > 1.0 + tolerance,
            )
            .withColumn(
                "pip",
                f.when(
                    f.max("pipSumAboveCutoff").over(study),
                    f.exp(lbf_jl - sum_lbf_l),
                ).otherwise(
                    f.col("pip"),
                ),
            )
            .drop("pipSumAboveCutoff")
        )

    @classmethod
    def _extract_credible_set_index(
        cls: type[EqtlCatalogueFinemapping], cs_id: Column
    ) -> Column:
        """Extract the credible set index from the cs_id.

        Args:
            cs_id (Column): column with the credible set id as defined in the eQTL Catalogue.

        Returns:
            Column: The credible set index.

        Examples:
            >>> spark.createDataFrame([("QTD000046_L1",)], ["cs_id"]).select(EqtlCatalogueFinemapping._extract_credible_set_index(f.col("cs_id"))).show()
            +----------------+
            |credibleSetIndex|
            +----------------+
            |               1|
            +----------------+
            <BLANKLINE>
        """
        return f.split(cs_id, "_L")[1].cast(IntegerType()).alias("credibleSetIndex")

    @classmethod
    def _extract_dataset_id_from_file_path(
        cls: type[EqtlCatalogueFinemapping], file_path: Column
    ) -> Column:
        """Extract the dataset_id from the file_path. The dataset_id follows the pattern QTD{6}.

        Args:
            file_path (Column): A column containing the file path.

        Returns:
            Column: The dataset_id.

        Examples:
            >>> spark.createDataFrame([("gs://bucket/susie/QTS000001/QTD000046/QTD000046.credible_sets.parquet",)], ["filename"]).select(EqtlCatalogueFinemapping._extract_dataset_id_from_file_path(f.col("filename"))).show()
            +----------+
            |dataset_id|
            +----------+
            | QTD000046|
            +----------+
            <BLANKLINE>
        """
        return f.regexp_extract(file_path, r"QTD\d{6}", 0).alias("dataset_id")

    @classmethod
    def _region_log_bayes_factors(
        cls: type[EqtlCatalogueFinemapping], lbf: DataFrame, credible_sets: DataFrame
    ) -> DataFrame:
        r"""Compute the log Bayes factor of each credible set over its whole region.

        For the $`l`$-th SuSiE component, the credible set Bayes factor is the log-sum-exp of the
        variant log Bayes factors with a uniform prior $`1/p`$ over the $`p`$ variants of the region:

        ```math
        \log\operatorname{BF}_l = \operatorname{LSE}_{j}(\operatorname{lbf}_{jl}) - \log p
        ```

        The `lbf_variable` file holds every variant of the region, not only the credible set members.

        Args:
            lbf (DataFrame): Log Bayes factors of every variant, as read by `read_lbf_from_source`.
            credible_sets (DataFrame): Credible set members, as read by `read_credible_set_from_source`.

        Returns:
            DataFrame: One row per credible set, keyed by `molecular_trait_id`, `region`, `dataset_id`
                and `credibleSetIndex`, with `credibleSetLogBayesFactor`.

        Examples:
            >>> lbf = spark.createDataFrame(
            ...     [("t1", "r1", "d1", 2.0, 0.0), ("t1", "r1", "d1", 0.0, 1.0)],
            ...     "molecular_trait_id string, region string, dataset_id string, lbf_variable1 double, lbf_variable2 double",
            ... )
            >>> for i in range(3, 11):
            ...     lbf = lbf.withColumn(f"lbf_variable{i}", f.lit(0.0))
            >>> credible_sets = spark.createDataFrame(
            ...     [("t1", "r1", "d1", 1)], "molecular_trait_id string, region string, dataset_id string, credibleSetIndex int"
            ... )
            >>> row = EqtlCatalogueFinemapping._region_log_bayes_factors(lbf, credible_sets).first()
            >>> round(row.credibleSetLogBayesFactor, 6)  # log((e^2 + e^0) / 2)
            1.433781
        """
        keys = ["molecular_trait_id", "region", "dataset_id"]
        lbf_l = f.element_at(
            f.array(*[f.col(f"lbf_variable{i}") for i in range(1, 11)]),
            f.col("credibleSetIndex"),
        )
        return (
            lbf.join(
                credible_sets.select(*keys, "credibleSetIndex").distinct(), on=keys
            )
            .groupBy(*keys, "credibleSetIndex")
            .agg(f.collect_list(lbf_l).alias("lbfs"))
            .select(
                *keys,
                "credibleSetIndex",
                region_log_bayes_factor(f.col("lbfs")).alias(
                    "credibleSetLogBayesFactor"
                ),
            )
        )

    @classmethod
    def parse_susie_results(
        cls: type[EqtlCatalogueFinemapping],
        credible_sets: DataFrame,
        lbf: DataFrame,
        studies_metadata: DataFrame,
        ss_ftp_path_template: str = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/r8_beta/sumstats",
        full_susie: DataFrame | None = None,
    ) -> DataFrame:
        """Parse the SuSIE results into a DataFrame containing the finemapping statistics and metadata about the studies.

        Some source studies (e.g. CommonMind/QTS000008, Kim-Hellmuth/QTS000042) report the per-variant
        `pip` computed by SuSiE instead of the per-(credible-set x variant) alpha value, which makes the
        posterior probabilities within a credible set sum to more than 1.0. This is corrected via
        [`scale_pip`][gentropy.datasource.eqtl_catalogue.finemapping.EqtlCatalogueFinemapping.scale_pip],
        which recomputes alpha from the log Bayes factors for any study affected by this issue.

        `conditionalBeta` and `conditionalStandardError` are the posterior mean and standard
        deviation of the variant's effect in the credible set's own SuSiE effect, taken from
        `full_susie` (see `read_full_susie_from_source`); they are null without it, or for a
        member it does not hold.

        Args:
            credible_sets (DataFrame): DataFrame containing raw statistics of all variants in the credible sets.
            lbf (DataFrame): DataFrame containing the raw log Bayes Factors for all variants.
            studies_metadata (DataFrame): DataFrame containing the study metadata.
            ss_ftp_path_template (str, optional): eQTL Catalogue FTP path template for summary statistics. Defaults to "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/r8_beta/sumstats".
            full_susie (DataFrame | None, optional): Posterior effects, as read by `read_full_susie_from_source`. Defaults to None.

        Returns:
            DataFrame: Processed SuSIE results to contain metadata about the studies and the finemapping statistics.
        """
        region_bayes_factors = cls._region_log_bayes_factors(lbf, credible_sets)
        keys = ["molecular_trait_id", "region", "variant", "dataset_id"]
        if full_susie is None:
            full_susie = credible_sets.sparkSession.createDataFrame(
                [],
                "molecular_trait_id string, region string, variant string, dataset_id string, "
                "conditionalBetas array<double>, conditionalStandardErrors array<double>",
            )
        results = (
            lbf.join(
                credible_sets.join(f.broadcast(studies_metadata), on="dataset_id"),
                on=[
                    "molecular_trait_id",
                    "region",
                    "variant",
                    "dataset_id",
                ],
                how="inner",
            )
            .withColumn(
                "logBF",
                f.when(f.col("credibleSetIndex") == 1, f.col("lbf_variable1"))
                .when(f.col("credibleSetIndex") == 2, f.col("lbf_variable2"))
                .when(f.col("credibleSetIndex") == 3, f.col("lbf_variable3"))
                .when(f.col("credibleSetIndex") == 4, f.col("lbf_variable4"))
                .when(f.col("credibleSetIndex") == 5, f.col("lbf_variable5"))
                .when(f.col("credibleSetIndex") == 6, f.col("lbf_variable6"))
                .when(f.col("credibleSetIndex") == 7, f.col("lbf_variable7"))
                .when(f.col("credibleSetIndex") == 8, f.col("lbf_variable8"))
                .when(f.col("credibleSetIndex") == 9, f.col("lbf_variable9"))
                .when(f.col("credibleSetIndex") == 10, f.col("lbf_variable10")),
            )
            .transform(cls.scale_pip)
            .join(full_susie, on=keys, how="left")
            .join(
                region_bayes_factors,
                on=["molecular_trait_id", "region", "dataset_id", "credibleSetIndex"],
                how="left",
            )
            .select(
                f.regexp_replace(f.col("variant"), r"chr", "").alias("variantId"),
                f.col("region"),
                normalize_chromosome(f.col("chromosome")).alias("chromosome"),
                f.col("position"),
                # The region is chrN:start-end:
                f.regexp_extract(f.col("region"), r":(\d+)-", 1)
                .cast(IntegerType())
                .alias("locusStart"),
                f.regexp_extract(f.col("region"), r"-(\d+)$", 1)
                .cast(IntegerType())
                .alias("locusEnd"),
                f.col("cs_min_r2").alias("purityMinR2"),
                f.col("credibleSetLogBayesFactor"),
                f.col("pip").alias("posteriorProbability"),
                *split_pvalue_column(f.col("pvalue")),
                f.col("sample_size").alias("nSamples"),
                f.col("beta"),
                f.col("se").alias("standardError"),
                f.element_at("conditionalBetas", f.col("credibleSetIndex")).alias(
                    "conditionalBeta"
                ),
                f.element_at(
                    "conditionalStandardErrors", f.col("credibleSetIndex")
                ).alias("conditionalStandardError"),
                f.col("credibleSetIndex"),
                f.col("logBF"),
                # Study metadata
                f.col("molecular_trait_id").alias("traitFromSource"),
                f.col("gene_id").alias("geneId"),
                f.col("dataset_id"),
                # Upon creation, the studyId cleaned from symbols:
                clean_strings_from_symbols(
                    f.concat_ws(
                        "_",
                        f.col("study_id"),
                        f.col("dataset_id"),
                        f.col("study_label"),
                        f.col("quant_method"),
                        f.col("sample_group"),
                        f.col("molecular_trait_id"),
                    )
                ).alias("studyId"),
                f.col("tissue_id").alias("biosampleFromSourceId"),
                EqtlCatalogueStudyIndex._identify_study_type().alias("studyType"),
                f.col("study_label").alias("projectId"),
                f.concat_ws(
                    "/",
                    f.lit(ss_ftp_path_template),
                    f.col("study_id"),
                    f.col("dataset_id"),
                ).alias("summarystatsLocation"),
                f.lit(True).alias("hasSumstats"),
                f.col("molecular_trait_id"),
                f.col("pmid").alias("pubmedId"),
                f.col("condition_label").alias("condition"),
            )
        )
        return results

    @classmethod
    def from_susie_results(
        cls: type[EqtlCatalogueFinemapping],
        processed_finemapping_df: DataFrame,
        lead_pvalue_threshold: float | None = None,
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Create the credible sets and their variants from the processed SuSIE results.

        A credible set is one SuSiE component of one molecular trait in one region. Its lead is the
        member with the highest posterior probability, ties broken by variant id. The reported
        statistics are the marginal ones the study reported, and the conditional ones the SuSiE
        posterior of the credible set's own effect, unscaled (see `read_full_susie_from_source`);
        the rescaled statistics are null. So are `r2Overall`, the allele frequency, which the
        source only gives as a minor allele frequency, and the sample size, as the source gives no
        per-variant sample size of the marginal test.

        Args:
            processed_finemapping_df (DataFrame): DataFrame containing the processed SuSIE results.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above it.
                Defaults to None, which keeps every credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: eQTL Catalogue credible sets and their variants.
        """
        members = processed_finemapping_df.withColumns(
            {
                "fineMappingMethod": f.lit(FinemappingMethod.SUSIE.value),
                "reportedBeta": f.col("beta"),
                "reportedStandardError": f.col("standardError"),
                "logBayesFactor": f.col("logBF"),
            }
        )
        return CredibleSet.from_members(
            members,
            ["dataset_id", "molecular_trait_id", "region", "credibleSetIndex"],
            lead_pvalue_threshold=lead_pvalue_threshold,
        )

    @classmethod
    def read_credible_set_from_source(
        cls: type[EqtlCatalogueFinemapping],
        credible_set_path: str | list[str],
        session: Session | None = None,
    ) -> DataFrame:
        """Load raw credible sets from eQTL Catalogue.

        Variants whose `ref`/`alt` are not purely A/T/G/C (e.g. `<DEL>`, `<DUP>` CNV notation used
        for large structural variants) are dropped, as they are reported with imprecise coordinates
        and were the source of duplicate credible-set entries (e.g. AFR_LCL/QTS000044, MAGE/QTS000055).
        The `ref`/`alt` columns are only needed to apply this filter and are not present in the output.

        Args:
            credible_set_path (str | list[str]): Path(s) or glob to parquet files containing finemapping results for any variant belonging to a credible set.
            session (Session | None, optional): Session object. If not provided, the method will try to find an active session. Defaults to None.

        Returns:
            DataFrame: Credible sets DataFrame.
        """
        session = session or Session.find()
        return (
            session.load_data(
                credible_set_path,
                fmt=NativeFileFormat.PARQUET.value,
                schema=cls.raw_credible_set_schema,
            )
            .select(
                "molecular_trait_id",
                "chromosome",
                "position",
                "ref",
                "alt",
                "variant",
                "pvalue",
                "beta",
                "se",
                "pip",
                "cs_id",
                "cs_min_r2",
                "region",
                "gene_id",
                # Adding dataset id based on the input file name:
                cls._extract_dataset_id_from_file_path(f.input_file_name()),
                # Parsing credible set index from the cs_id:
                cls._extract_credible_set_index(f.col("cs_id")),
            )
            # Remove non-ATGC alleles, this removes the <DEL>, <DUP> variants:
            .filter(flag_non_atgc_alleles(f.col("ref"), f.col("alt")))
            .drop("ref", "alt")
            # Remove duplicates caused by explosion of single variants to multiple rsid-s:
            .distinct()
        )

    @classmethod
    def read_full_susie_from_source(
        cls: type[EqtlCatalogueFinemapping],
        full_susie_path: str | list[str],
        session: Session | None = None,
    ) -> DataFrame:
        r"""Load the SuSiE posterior effects from eQTL Catalogue.

        The `*.full_susie.parquet` files hold, for every variant of a fine-mapped region and every
        SuSiE effect $`l`$, the first and second moments $`\mu_l`$ and $`\mu^{(2)}_l`$ of the
        variant's effect given that it is the causal variant of effect $`l`$. They are on the scale
        SuSiE was fitted on, per standard deviation of the genotype: dividing them by the file's
        `X_column_scale_factors` gives per-allele effects, which is left to a later rescaling.
        Only the credible set members (`cs_index` set) are kept; a member of two credible sets
        keeps the moments of both effects.

        Args:
            full_susie_path (str | list[str]): Path(s) or glob to the `*.full_susie.parquet` files.
            session (Session | None, optional): Session object. If not provided, the method will try to find an active session. Defaults to None.

        Returns:
            DataFrame: One row per credible set member, keyed by `molecular_trait_id`, `region`,
                `variant` and `dataset_id`, with `conditionalBetas`, the posterior means
                $`\mu_l`$, and `conditionalStandardErrors`, the posterior standard deviations
                $`\sqrt{\mu^{(2)}_l - \mu_l^2}`$, indexed by effect.

        Examples:
            >>> import pandas as pd, tempfile
            >>> row = {"molecular_trait_id": "t1", "region": "r1", "variant": "chr1_10_A_T", "cs_index": "L1"}
            >>> row |= {f"mu_{i}": 0.0 for i in range(1, 11)} | {f"mu2_{i}": 0.0 for i in range(1, 11)}
            >>> row |= {"mu_1": 0.5, "mu2_1": 0.26}
            >>> path = tempfile.mkdtemp() + "/QTD000001.full_susie.parquet"
            >>> pd.DataFrame([row, row | {"variant": "chr1_20_C_G", "cs_index": None}]).to_parquet(path)
            >>> r = EqtlCatalogueFinemapping.read_full_susie_from_source(path).first()
            >>> (r.variant, r.dataset_id, r.conditionalBetas[0], round(r.conditionalStandardErrors[0], 6))
            ('chr1_10_A_T', 'QTD000001', 0.5, 0.1)
        """
        session = session or Session.find()
        return (
            session.load_data(
                full_susie_path,
                fmt=NativeFileFormat.PARQUET.value,
                schema=cls.raw_full_susie_schema,
            )
            .filter(f.col("cs_index").isNotNull())
            .select(
                "molecular_trait_id",
                "region",
                "variant",
                cls._extract_dataset_id_from_file_path(f.input_file_name()),
                f.array(*[f.col(f"mu_{i}") for i in range(1, 11)]).alias(
                    "conditionalBetas"
                ),
                # Clamped at 0, as the variance can come out slightly negative in floating point:
                f.array(
                    *[
                        f.sqrt(
                            f.greatest(
                                f.col(f"mu2_{i}") - f.col(f"mu_{i}") ** 2, f.lit(0.0)
                            )
                        )
                        for i in range(1, 11)
                    ]
                ).alias("conditionalStandardErrors"),
            )
            .distinct()
        )

    @classmethod
    def read_lbf_from_source(
        cls: type[EqtlCatalogueFinemapping],
        lbf_path: str | list[str],
        session: Session | None = None,
    ) -> DataFrame:
        """Load raw log Bayes Factors from eQTL Catalogue.

        Args:
            lbf_path (str | list[str]): Path(s) or glob to parquet files containing Log Bayes Factors for each variant.
            session (Session | None, optional): Session object. If not provided, the method will try to find an active session. Defaults to None.

        Returns:
            DataFrame: Log Bayes Factors DataFrame.
        """
        session = session or Session.find()
        return (
            session.load_data(
                lbf_path,
                fmt=NativeFileFormat.PARQUET.value,
                schema=cls.raw_lbf_schema,
            )
            .select(
                "molecular_trait_id",
                "region",
                "variant",
                *[f"lbf_variable{i}" for i in range(1, 11)],
                cls._extract_dataset_id_from_file_path(f.input_file_name()),
            )
            .distinct()
        )
