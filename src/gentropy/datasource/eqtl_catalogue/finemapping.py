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
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.datasource.eqtl_catalogue.study_index import EqtlCatalogueStudyIndex

if TYPE_CHECKING:
    pass


@dataclass
class EqtlCatalogueFinemapping:
    """SuSIE finemapping dataset for eQTL Catalogue.

    Credible sets from SuSIE are extracted and transformed into StudyLocus objects:
    - A study ID is defined as a triad between: the publication, the tissue, and the measured trait (e.g. Braineac2_substantia_nigra_ENSG00000248275)
    - Each row in the `*.credible_set.parquet` files is represented by molecular_trait_id/variant/rsid trios relevant for a given tissue. Each have their own finemapping statistics
    - log Bayes Factors are available for all variants in the `*.lbf_variable.parquet` files
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
            DataFrame: DataFrame with alpha columns added.
        """
        # First we compute the boolean condition checking if PIP sum over single credible
        # set variants ia above 1.0 + tolerance
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
    def parse_susie_results(
        cls: type[EqtlCatalogueFinemapping],
        credible_sets: DataFrame,
        lbf: DataFrame,
        studies_metadata: DataFrame,
        ss_ftp_path_template: str = "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/r8_beta/sumstats",
    ) -> DataFrame:
        """Parse the SuSIE results into a DataFrame containing the finemapping statistics and metadata about the studies.

        Some source studies (e.g. CommonMind/QTS000008, Kim-Hellmuth/QTS000042) report the per-variant
        `pip` computed by SuSiE instead of the per-(credible-set x variant) alpha value, which makes the
        posterior probabilities within a credible set sum to more than 1.0. This is corrected via
        [`scale_pip`][gentropy.datasource.eqtl_catalogue.finemapping.EqtlCatalogueFinemapping.scale_pip],
        which recomputes alpha from the log Bayes factors for any study affected by this issue.

        Args:
            credible_sets (DataFrame): DataFrame containing raw statistics of all variants in the credible sets.
            lbf (DataFrame): DataFrame containing the raw log Bayes Factors for all variants.
            studies_metadata (DataFrame): DataFrame containing the study metadata.
            ss_ftp_path_template (str, optional): eQTL Catalogue FTP path template for summary statistics. Defaults to "https://ftp.ebi.ac.uk/pub/databases/spot/eQTL/r8_beta/sumstats".

        Returns:
            DataFrame: Processed SuSIE results to contain metadata about the studies and the finemapping statistics.
        """
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
            .select(
                f.regexp_replace(f.col("variant"), r"chr", "").alias("variantId"),
                f.col("region"),
                normalize_chromosome(f.col("chromosome")).alias("chromosome"),
                f.col("position"),
                f.col("pip").alias("posteriorProbability"),
                *split_pvalue_column(f.col("pvalue")),
                f.col("sample_size").alias("nSamples"),
                f.col("beta"),
                f.col("se").alias("standardError"),
                f.col("credibleSetIndex"),
                f.col("logBF"),
                f.lit(FinemappingMethod.SUSIE.value).alias("finemappingMethod"),
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
        cls: type[EqtlCatalogueFinemapping], processed_finemapping_df: DataFrame
    ) -> StudyLocus:
        """Create a StudyLocus object from the processed SuSIE results.

        Args:
            processed_finemapping_df (DataFrame): DataFrame containing the processed SuSIE results.

        Returns:
            StudyLocus: eQTL Catalogue credible sets.
        """
        lead_w = Window.partitionBy(
            "dataset_id", "molecular_trait_id", "region", "credibleSetIndex"
        )
        study_locus_cols = [
            field.name
            for field in StudyLocus.get_schema().fields
            if field.name in processed_finemapping_df.columns
        ] + ["locus"]
        return StudyLocus(
            _df=(
                processed_finemapping_df.withColumn(
                    "isLead",
                    f.row_number().over(lead_w.orderBy(f.desc("posteriorProbability")))
                    == f.lit(1),
                )
                .withColumn(
                    # Collecting all variants that constitute the credible set brings as many variants as the credible set size
                    "locus",
                    f.when(
                        f.col("isLead"),
                        f.collect_list(
                            f.struct(
                                "variantId",
                                "posteriorProbability",
                                "pValueMantissa",
                                "pValueExponent",
                                "logBF",
                                "beta",
                                "standardError",
                            )
                        ).over(lead_w),
                    ),
                )
                .filter(f.col("isLead"))
                .drop("isLead")
                .select(
                    *study_locus_cols,
                    StudyLocus.assign_study_locus_id(
                        ["studyId", "variantId", "finemappingMethod"]
                    ),
                    StudyLocus.calculate_credible_set_log10bf(
                        f.col("locus.logBF")
                    ).alias("credibleSetlog10BF"),
                )
            ),
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()

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
