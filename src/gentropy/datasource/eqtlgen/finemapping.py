"""Datasource ingestion: eQTLGen single-cell eQTL and co-eQTL fine-mapping (SuSiE) to credible sets."""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame, Window

from gentropy.common.processing import normalize_chromosome
from gentropy.common.session import Session
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.credible_set import CredibleSet, CredibleSetVariant
from gentropy.dataset.study_index import StudyType
from gentropy.dataset.study_locus import FinemappingMethod


class EqtlGenDataset(Enum):
    """The eQTLGen fine-mapping datasets.

    Attributes:
        SINGLE_CELL_EQTL (str): cis-eQTLs, one study per cell type and gene.
        COEQTL (str): co-expression QTLs, one study per cell type and gene pair.
    """

    SINGLE_CELL_EQTL = "single_cell_eqtl"
    COEQTL = "coeqtl"


@dataclass
class EqtlGenFinemapping:
    """SuSiE fine-mapping of the eQTLGen consortium single-cell eQTLs and co-eQTLs.

    eQTLGen provides one tab-separated file per cell type and chromosome, with one row per
    variant tested for a feature: the marginal statistics, the SuSiE credible set of the variant
    (`SusieRss_CS`, `NA` outside of a credible set) and the natural log Bayes factors of the 5
    SuSiE single effects (`lbf_cs_1` to `lbf_cs_5`). The cell type is the folder of the file:

    - single-cell eQTLs: `finemap_<cell type>/`, one feature per gene (`ENSG`);
    - co-eQTLs: `joint_co_eQTL_fine_mapping_<cell type>_SF_coloc_<date>/`, one feature per pair
      of the gene with the eQTL (`eGene`) and the co-expressed gene (`coeGene`), both given as
      gene symbols. Only z-scores are given, no effect sizes.

    `snp_id` is `chromosome:position:reference:alternative` on GRCh38, and the statistics are
    those of `assessed_allele`, which is the reference allele. The effect sizes are turned to the
    alternative allele, the effect allele of `variantId`.

    An example row of the single-cell eQTL files (columns not used are not shown):

    | feature_id | snp_id | p_value | beta | beta_se | ENSG | n_samples | snp_chromosome | snp_position | assessed_allele | SusieRss_CS | lbf_cs_1 | ... | lbf_cs_5 |
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|
    | OPTN | 10:12112834:C:T | 0.2485 | -0.0419 | 0.0363 | ENSG00000123240 | 1962 | 10 | 12112834 | C | NA | -1.647 | ... | 0 |

    An example row of the co-eQTL files (columns not used are not shown):

    | eGene | coeGene | feature_chromosome | feature_start | feature_end | snp_id | snp_chromosome | snp_position | assessed_allele | p_value | n_samples | SusieRss_CS | lbf_cs_1 | ... | lbf_cs_5 |
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
    | CORO1B | BACH2 | 11 | 67435510 | 67443821 | 11:66435554:A:C | 11 | 66435554 | A | 0.2565 | 265 | NA | -1.001 | ... | 0 |
    """

    PROJECT_ID = "eQTLGen"
    N_EFFECTS = 5

    # The cell type of a file, from its folder.
    SINGLE_CELL_EQTL_FOLDER = r"finemap_([^/]+)/[^/]+$"
    COEQTL_FOLDER = r"joint_co_eQTL_fine_mapping_(.+?)_SF_coloc_[^/]+/[^/]+$"

    # The co-eQTL study identifiers of the previous releases name the T cells without `_T`.
    COEQTL_STUDY_CELL_TYPES = {"CD4_T": "CD4", "CD8_T": "CD8"}

    @staticmethod
    def cell_type(pattern: str) -> Column:
        """Cell type of the row, from the folder of its file.

        Args:
            pattern (str): Regular expression with the cell type as its first group.

        Returns:
            Column: Cell type, e.g. `CD4_T`.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [("gs://b/raw_data/joint_co_eQTL_fine_mapping_CD4_T_SF_coloc_20250204/f_1.txt",)], ["path"]
            ... )
            >>> df.select(
            ...     f.regexp_extract("path", EqtlGenFinemapping.COEQTL_FOLDER, 1).alias("cellType")
            ... ).show()
            +--------+
            |cellType|
            +--------+
            |   CD4_T|
            +--------+
            <BLANKLINE>
        """
        return f.regexp_extract(f.input_file_name(), pattern, 1)

    @staticmethod
    def _read(session: Session, files: str | list[str]) -> DataFrame:
        """Read the tab-separated files, with every column as a string and `NA` as null.

        Args:
            session (Session): Session object.
            files (str | list[str]): Paths or globs of the files.

        Returns:
            DataFrame: The rows of the files.
        """
        return (
            session.spark.read.option("delimiter", "\t")
            .option("header", True)
            .option("nullValue", "NA")
            .csv(files)
        )

    @classmethod
    def _variant_columns(cls: type[EqtlGenFinemapping]) -> list[Column]:
        """Columns shared by both datasets: the variant, its p-value and the SuSiE results.

        Returns:
            list[Column]: The columns.
        """
        return [
            normalize_chromosome(f.col("snp_chromosome")).alias("chromosome"),
            f.col("snp_position").cast(t.IntegerType()).alias("position"),
            f.regexp_replace(f.col("snp_id"), ":", "_").alias("variantId"),
            f.col("p_value").alias("pValue"),
            f.col("n_samples").cast(t.IntegerType()).alias("sampleSize"),
            f.col("SusieRss_CS").cast(t.IntegerType()).alias("variantCredibleSetIndex"),
            *[
                f.col(f"lbf_cs_{i}").cast(t.DoubleType()).alias(f"lbf_cs_{i}")
                for i in range(1, cls.N_EFFECTS + 1)
            ],
        ]

    @classmethod
    def read_single_cell_eqtl(
        cls: type[EqtlGenFinemapping], session: Session, files: str | list[str]
    ) -> DataFrame:
        """Read the single-cell eQTL files: every variant tested for a gene.

        The study is `eQTLGen_<cell type>_<gene>`. `beta` is the effect of the reference allele,
        so its sign is turned for the alternative allele.

        Args:
            session (Session): Session object.
            files (str | list[str]): Paths or globs of the files.

        Returns:
            DataFrame: One row per variant and gene.
        """
        return cls._read(session, files).select(
            cls.cell_type(cls.SINGLE_CELL_EQTL_FOLDER).alias("cellType"),
            f.col("ENSG").alias("geneId"),
            f.col("feature_id").alias("traitFromSource"),
            *cls._variant_columns(),
            (-f.col("beta").cast(t.DoubleType())).alias("reportedBeta"),
            f.col("beta_se").cast(t.DoubleType()).alias("reportedStandardError"),
        ).withColumn(
            "studyId",
            f.concat_ws(
                "_", f.lit(cls.PROJECT_ID), f.col("cellType"), f.col("geneId")
            ),
        )

    @classmethod
    def read_coeqtl(
        cls: type[EqtlGenFinemapping], session: Session, files: str | list[str]
    ) -> DataFrame:
        """Read the co-eQTL files: every variant tested for a pair of genes.

        The study is `eQTLGen_<cell type>_<eGene>_<coeGene>`, with the CD4 and CD8 T cells
        written `CD4` and `CD8` as in the previous releases. The files give no effect size.

        Args:
            session (Session): Session object.
            files (str | list[str]): Paths or globs of the files.

        Returns:
            DataFrame: One row per variant and pair of genes, with the location of `eGene`.
        """
        cell_type = cls.cell_type(cls.COEQTL_FOLDER)
        study_cell_type = f.coalesce(
            f.create_map(
                *[f.lit(x) for kv in cls.COEQTL_STUDY_CELL_TYPES.items() for x in kv]
            )[cell_type],
            cell_type,
        )
        return cls._read(session, files).select(
            cell_type.alias("cellType"),
            f.concat_ws(
                "_",
                f.lit(cls.PROJECT_ID),
                study_cell_type,
                f.col("eGene"),
                f.col("coeGene"),
            ).alias("studyId"),
            f.col("eGene"),
            f.col("coeGene"),
            f.concat_ws("_", f.col("eGene"), f.col("coeGene")).alias(
                "traitFromSource"
            ),
            normalize_chromosome(f.col("feature_chromosome")).alias(
                "featureChromosome"
            ),
            f.col("feature_start").cast(t.IntegerType()).alias("featureStart"),
            f.col("feature_end").cast(t.IntegerType()).alias("featureEnd"),
            *cls._variant_columns(),
        )

    @classmethod
    def read(
        cls: type[EqtlGenFinemapping],
        session: Session,
        dataset: EqtlGenDataset,
        files: str | list[str],
    ) -> DataFrame:
        """Read the files of a dataset.

        Args:
            session (Session): Session object.
            dataset (EqtlGenDataset): The dataset of the files.
            files (str | list[str]): Paths or globs of the files.

        Returns:
            DataFrame: One row per variant and feature.
        """
        if dataset is EqtlGenDataset.SINGLE_CELL_EQTL:
            return cls.read_single_cell_eqtl(session, files)
        return cls.read_coeqtl(session, files)

    @classmethod
    def members(
        cls: type[EqtlGenFinemapping],
        variants: DataFrame,
        min_credible_set_log10_bayes_factor: float,
    ) -> DataFrame:
        r"""Credible set members, with the statistics of their credible set's single effect.

        The members of a credible set are the variants with its `SusieRss_CS`. `SusieRss_CS`
        numbers the credible sets of a feature, not the SuSiE single effects of `lbf_cs_1` to
        `lbf_cs_5`: the credible set 1 can be the effect 3. The effect of a credible set is the
        one in which its members have the largest posterior probability in total.

        With $`\operatorname{lbf}_{j}`$ the log Bayes factor of variant $`j`$ in that effect and
        a uniform prior over the $`p`$ variants tested for the feature:

        - the posterior probability of a member is the softmax of the log Bayes factors of the
          effect over every variant tested, $`e^{\operatorname{lbf}_{j}} / \sum_{i} e^{\operatorname{lbf}_{i}}`$;
        - the credible set log Bayes factor is $`\operatorname{LSE}_{j}(\operatorname{lbf}_{j}) - \log p`$,
          as `gentropy.common.stats.region_log_bayes_factors`;
        - `locusStart` and `locusEnd` are the first and last variant tested.

        Credible sets with a log10 Bayes factor below `min_credible_set_log10_bayes_factor`
        are dropped.

        Args:
            variants (DataFrame): Every variant tested, as given by `read`.
            min_credible_set_log10_bayes_factor (float): Minimum log10 Bayes factor of a credible set.

        Returns:
            DataFrame: One row per member, with the columns of `CredibleSet.from_members`.

        Examples:
            >>> variants = spark.createDataFrame(
            ...     [("v1", 1, 0.0, 9.0), ("v2", None, 5.0, 0.0), ("v3", None, 0.0, 0.0)],
            ...     "variantId string, variantCredibleSetIndex int, lbf_cs_1 double, lbf_cs_2 double",
            ... ).withColumns({
            ...     "studyId": f.lit("S1"), "position": f.lit(1), "pValue": f.lit("1e-9"),
            ...     **{f"lbf_cs_{i}": f.lit(0.0) for i in range(3, 6)},
            ... })
            >>> EqtlGenFinemapping.members(variants, 0.0).select(
            ...     "variantId", "credibleSetIndex", "logBayesFactor", f.round("posteriorProbability", 4).alias("pp")
            ... ).show()  # the credible set 1 is the effect 2
            +---------+----------------+--------------+------+
            |variantId|credibleSetIndex|logBayesFactor|    pp|
            +---------+----------------+--------------+------+
            |       v1|               1|           9.0|0.9998|
            +---------+----------------+--------------+------+
            <BLANKLINE>
        """
        effects = range(1, cls.N_EFFECTS + 1)
        study = Window.partitionBy("studyId")
        credible_set = Window.partitionBy("studyId", "credibleSetIndex")

        def lbf(i: int) -> Column:
            return f.col(f"lbf_cs_{i}")

        # Per effect, over every variant tested for the feature: the maximum log Bayes factor,
        # the sum of the Bayes factors scaled by it, and the number of variants.
        with_effects = variants.withColumns(
            {
                "locusStart": f.min("position").over(study),
                "locusEnd": f.max("position").over(study),
                **{f"_max{i}": f.max(lbf(i)).over(study) for i in effects},
            }
        ).withColumns(
            {
                **{
                    f"_sum{i}": f.sum(f.exp(lbf(i) - f.col(f"_max{i}"))).over(study)
                    for i in effects
                },
                **{f"_n{i}": f.count(lbf(i)).over(study) for i in effects},
            }
        )

        def alpha(i: int) -> Column:
            return f.exp(lbf(i) - f.col(f"_max{i}")) / f.col(f"_sum{i}")

        def per_effect(column: str) -> Column:
            return f.element_at(f.col(column), f.col("_effect"))

        members = (
            with_effects.filter(f.col("variantCredibleSetIndex").isNotNull())
            .withColumnRenamed("variantCredibleSetIndex", "credibleSetIndex")
            .withColumns(
                {
                    "_alpha": f.array(*[alpha(i) for i in effects]),
                    "_lbf": f.array(*[lbf(i) for i in effects]),
                    "_credibleSetLbf": f.array(
                        *[
                            f.col(f"_max{i}")
                            + f.log(f.col(f"_sum{i}"))
                            - f.log(f.col(f"_n{i}"))
                            for i in effects
                        ]
                    ),
                    "_mass": f.array(
                        *[f.sum(alpha(i)).over(credible_set) for i in effects]
                    ),
                }
            )
            .withColumn(
                "_effect",
                f.expr("array_position(_mass, array_max(_mass))").cast("int"),
            )
            .withColumns(
                {
                    "posteriorProbability": per_effect("_alpha"),
                    "logBayesFactor": per_effect("_lbf"),
                    "credibleSetLogBayesFactor": per_effect("_credibleSetLbf"),
                }
            )
        )
        return (
            members.filter(
                f.col("credibleSetLogBayesFactor")
                >= min_credible_set_log10_bayes_factor * math.log(10)
            )
            .select(
                "*",
                *split_pvalue_column(f.col("pValue")),
                f.lit(StudyType.SCEQTL.value).alias("studyType"),
                f.lit(FinemappingMethod.SUSIE.value).alias("fineMappingMethod"),
            )
            .drop(
                "pValue",
                *[c for c in members.columns if c.startswith("_")],
            )
        )

    @classmethod
    def from_source(
        cls: type[EqtlGenFinemapping],
        variants: DataFrame,
        lead_pvalue_threshold: float | None = 1e-2,
        min_credible_set_log10_bayes_factor: float = 2 / math.log(10),
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Build credible sets and their variants from the variants of `read`.

        The defaults are the filters of the eQTLGen release `260326`: a lead p-value of at most
        0.01 (`pValueExponent <= -3`) and a credible set log10 Bayes factor of at least
        `2 / ln(10)`, a Bayes factor of `e^2`.

        - `pValueMantissa`, `pValueExponent`, `sampleSize`: the marginal statistics of the variant.
        - `reportedBeta`, `reportedStandardError`: the marginal effect of the alternative allele,
          for the single-cell eQTLs only.
        - `posteriorProbability`, `logBayesFactor`: of the credible set's single effect (see `members`).
        - `effectAlleleFrequency`, `conditionalBeta`, `conditionalStandardError`, `r2Overall`
          and the purity of the credible sets are not given by eQTLGen.

        Args:
            variants (DataFrame): Every variant tested, as given by `read`.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above it.
            min_credible_set_log10_bayes_factor (float): Minimum log10 Bayes factor of a credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: Credible sets and their variants.
        """
        return CredibleSet.from_members(
            cls.members(variants, min_credible_set_log10_bayes_factor),
            ["studyId", "credibleSetIndex"],
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
