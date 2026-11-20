"""Datasource ingestion: SLE-map single-cell eQTL fine-mapping (SuSiE) to credible sets."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame

from gentropy.common.processing import normalize_chromosome
from gentropy.common.session import Session
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_index import StudyType
from gentropy.dataset.study_locus import FinemappingMethod


@dataclass
class SleMapFinemapping:
    """SuSiE fine-mapping of the SLE-map single-cell cis-eQTLs.

    SLE-map maps cis-eQTLs in 15 peripheral blood cell types, and in all cells together, of a
    systemic lupus erythematosus cohort, and fine-maps every cell type and gene with SuSiE
    (`susieR`, up to 10 single effects, 95% credible sets) on individual-level genotypes. The
    results are three tab-separated tables, with `NA` for missing values:

    - `all_components.tsv`: one row per credible set (a SuSiE single effect, `component`), with
      its purity, the window of the variants tested for the gene (`analyzed_start`,
      `analyzed_end`) and the natural log Bayes factor of the single effect over every variant
      tested (`component_region_logbf`).
    - `all_memberships.tsv`: one row per credible set member, with the marginal statistics of the
      alternative allele (`beta`, `standard_error`, `pval_nominal`, `af`) and the posterior
      probability (`component_pip`) and natural log Bayes factor (`component_logbf`) of the
      variant in the single effect.
    - `all_diagnostics.tsv`: one row per cell type and gene fitted, with its sample size.

    `variant_id` is `chr<chromosome>_<position>_<reference>_<alternative>` on GRCh38.

    `conditional_beta` and `conditional_sd` of the memberships are the posterior mean and
    standard deviation of the effect given that the variant is the causal one of the single
    effect, per standard deviation of genotype and expression, unlike the per-allele `beta`.

    An example row of the components (`all_components.tsv`):

    | cell_type | gene | region | component | coverage | n_members | n_analyzed_variants | analyzed_start | analyzed_end | purity_min_abs_corr | purity_mean_abs_corr | purity_min_r2 | purity_mean_r2 | component_region_logbf | prior_variance |
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
    | EM_CD8_T_cells | ENSG00000137331 | 6:29744927-31744419 | 1 | 0.99384 | 3 | 15061 | 29744927 | 31744419 | 0.71925 | 0.81169 | 0.51732 | 0.67403 | 7.71646 | 0.11741 |

    An example row of the memberships (`all_memberships.tsv`):

    | cell_type | gene | region | variant_id | chr | pos | ref | alt | z | beta | standard_error | pval_nominal | af | component | component_pip | component_logbf | conditional_beta | conditional_sd | coverage |
    |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
    | Naive_CD4_T_cells | ENSG00000051108 | 16:55932392-57930074 | chr16_56719719_C_T | chr16 | 56719719 | C | T | 4.60703 | 0.018946 | 0.004112 | 6.5962e-06 | 0.17766 | 1 | 0.02387 | 8.67697 | 0.25475 | 0.05628 | 0.95272 |

    An example row of the diagnostics (`all_diagnostics.tsv`, columns not used are not shown):

    | cell_type | gene | status | n_samples | n_variants_sumstats | n_cs | converged |
    |---|---|---|---|---|---|---|
    | CM_CD8_T_cells | ENSG00000105329 | ok | 148 | 2623 | 1 | TRUE |
    """

    PROJECT_ID: str = "OTAR2064"
    STUDY_ID_PREFIX: str = "SLEmap_sceqtl"

    components_schema: t.StructType = t.StructType(
        [
            t.StructField("cell_type", t.StringType(), True),
            t.StructField("gene", t.StringType(), True),
            t.StructField("region", t.StringType(), True),
            t.StructField("component", t.IntegerType(), True),
            t.StructField("coverage", t.DoubleType(), True),
            t.StructField("n_members", t.IntegerType(), True),
            t.StructField("n_analyzed_variants", t.IntegerType(), True),
            t.StructField("analyzed_start", t.IntegerType(), True),
            t.StructField("analyzed_end", t.IntegerType(), True),
            t.StructField("purity_min_abs_corr", t.DoubleType(), True),
            t.StructField("purity_mean_abs_corr", t.DoubleType(), True),
            t.StructField("purity_min_r2", t.DoubleType(), True),
            t.StructField("purity_mean_r2", t.DoubleType(), True),
            t.StructField("component_region_logbf", t.DoubleType(), True),
            t.StructField("prior_variance", t.DoubleType(), True),
        ]
    )

    memberships_schema: t.StructType = t.StructType(
        [
            t.StructField("cell_type", t.StringType(), True),
            t.StructField("gene", t.StringType(), True),
            t.StructField("region", t.StringType(), True),
            t.StructField("variant_id", t.StringType(), True),
            t.StructField("chr", t.StringType(), True),
            t.StructField("pos", t.IntegerType(), True),
            t.StructField("ref", t.StringType(), True),
            t.StructField("alt", t.StringType(), True),
            t.StructField("z", t.DoubleType(), True),
            t.StructField("beta", t.DoubleType(), True),
            t.StructField("standard_error", t.DoubleType(), True),
            # A string, so that split_pvalue_column keeps the precision of the source:
            t.StructField("pval_nominal", t.StringType(), True),
            t.StructField("af", t.DoubleType(), True),
            t.StructField("component", t.IntegerType(), True),
            t.StructField("component_pip", t.DoubleType(), True),
            t.StructField("component_logbf", t.DoubleType(), True),
            t.StructField("conditional_beta", t.DoubleType(), True),
            t.StructField("conditional_sd", t.DoubleType(), True),
            t.StructField("coverage", t.DoubleType(), True),
        ]
    )

    @staticmethod
    def _read_tsv(
        session: Session, path: str | list[str], schema: t.StructType
    ) -> DataFrame:
        """Read a tab-separated table of SLE-map with a header and `NA` for missing values.

        Args:
            session (Session): Session object.
            path (str | list[str]): Path to the table.
            schema (t.StructType): Schema of its leading columns.

        Returns:
            DataFrame: The table.
        """
        return (
            session.spark.read.schema(schema)
            .option("delimiter", "\t")
            .option("header", True)
            .option("nullValue", "NA")
            .csv(path)
        )

    @classmethod
    def build_study_id(cls: type[SleMapFinemapping], cell_type: Column, gene_id: Column) -> Column:
        """Build the study identifier from the cell type and gene.

        Args:
            cell_type (Column): Cell type, e.g. `EM_CD8_T_cells`.
            gene_id (Column): Ensembl gene identifier.

        Returns:
            Column: Study identifier.

        Examples:
            >>> df = spark.createDataFrame([("EM_CD8_T_cells", "ENSG00000137331")], ["cellType", "geneId"])
            >>> df.select(
            ...     SleMapFinemapping.build_study_id(f.col("cellType"), f.col("geneId")).alias("studyId")
            ... ).show(truncate=False)
            +--------------------------------------------+
            |studyId                                     |
            +--------------------------------------------+
            |SLEmap_sceqtl_EM_CD8_T_cells_ENSG00000137331|
            +--------------------------------------------+
            <BLANKLINE>
        """
        return f.concat_ws("_", f.lit(cls.STUDY_ID_PREFIX), cell_type, gene_id)

    @classmethod
    def read_components(
        cls: type[SleMapFinemapping],
        session: Session,
        components_path: str | list[str],
        purity_min_r2_threshold: float | None,
    ) -> DataFrame:
        """Read the credible sets and keep the pure enough ones.

        Args:
            session (Session): Session object.
            components_path (str | list[str]): Path to `all_components.tsv`.
            purity_min_r2_threshold (float | None): Minimum pairwise r2 within the credible set.
                None keeps every credible set.

        Returns:
            DataFrame: One row per credible set, keyed by `cellType`, `geneId` and `credibleSetIndex`.
        """
        components = cls._read_tsv(session, components_path, cls.components_schema).select(
            f.col("cell_type").alias("cellType"),
            f.col("gene").alias("geneId"),
            f.col("component").alias("credibleSetIndex"),
            f.col("analyzed_start").alias("locusStart"),
            f.col("analyzed_end").alias("locusEnd"),
            f.col("purity_mean_r2").alias("purityMeanR2"),
            f.col("purity_min_r2").alias("purityMinR2"),
            f.col("component_region_logbf").alias("credibleSetLogBayesFactor"),
        )
        if purity_min_r2_threshold is not None:
            components = components.filter(
                f.col("purityMinR2") >= purity_min_r2_threshold
            )
        return components

    @classmethod
    def read_sample_sizes(
        cls: type[SleMapFinemapping],
        session: Session,
        diagnostics_path: str | list[str],
    ) -> DataFrame:
        """Read the number of samples of every cell type and gene fitted.

        Args:
            session (Session): Session object.
            diagnostics_path (str | list[str]): Path to `all_diagnostics.tsv`.

        Returns:
            DataFrame: `cellType`, `geneId` and `nSamples`.
        """
        return (
            session.spark.read.option("delimiter", "\t")
            .option("header", True)
            .option("nullValue", "NA")
            .csv(diagnostics_path)
            .select(
                f.col("cell_type").alias("cellType"),
                f.col("gene").alias("geneId"),
                f.col("n_samples").cast(t.IntegerType()).alias("nSamples"),
            )
        )

    @classmethod
    def from_source(
        cls: type[SleMapFinemapping],
        session: Session,
        memberships_path: str | list[str],
        components_path: str | list[str],
        diagnostics_path: str | list[str],
        purity_min_r2_threshold: float | None = 0.25,
        lead_pvalue_threshold: float | None = 1e-3,
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Build credible sets and their variants from the SLE-map SuSiE results.

        - `reportedBeta`, `reportedStandardError`, `pValueMantissa`, `pValueExponent`: the
          marginal `beta`, `standard_error` and `pval_nominal` of the alternative allele.
        - `effectAlleleFrequency`: `af`, the frequency of the alternative allele, flagged
          `EAF_FROM_SOURCE`.
        - `sampleSize`: `n_samples` of the diagnostics of the cell type and gene.
        - `posteriorProbability`, `logBayesFactor`: `component_pip` and `component_logbf`, of the
          credible set's single effect.
        - Credible set `logBayesFactor`: `component_region_logbf`, the Bayes factor of the single
          effect over every variant tested for the gene, as `gentropy.common.stats.region_log_bayes_factors`.
        - `purityMinR2`, `purityMeanR2`: `purity_min_r2` and `purity_mean_r2`.
        - `locusStart`, `locusEnd`: `analyzed_start` and `analyzed_end`, the window of the
          variants tested for the gene.
        - `conditionalBeta`, `conditionalStandardError`: `conditional_beta` and `conditional_sd`,
          on SuSiE's scale, per standard deviation of genotype and expression (see the
          `gentropy.dataset.credible_set` module), not rescaled.
        - `r2Overall` is left empty: SLE-map gives no LD with the lead.

        Args:
            session (Session): Session object.
            memberships_path (str | list[str]): Path to `all_memberships.tsv`.
            components_path (str | list[str]): Path to `all_components.tsv`.
            diagnostics_path (str | list[str]): Path to `all_diagnostics.tsv`.
            purity_min_r2_threshold (float | None): Drop the credible sets whose minimum pairwise r2
                is below it. Default 0.25, as for the FinnGen multiome ingestion. None keeps every
                credible set.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above
                it. Default 0.001, as in the earlier SLE-map release. None keeps every credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: Credible sets, one study per cell type and gene, and their variants.
        """
        keys = ["cellType", "geneId", "credibleSetIndex"]
        components = cls.read_components(
            session, components_path, purity_min_r2_threshold
        )
        sample_sizes = cls.read_sample_sizes(session, diagnostics_path)
        effect_allele_frequency = f.col("af").cast(t.FloatType())
        members = (
            cls._read_tsv(session, memberships_path, cls.memberships_schema)
            .withColumnsRenamed(
                {
                    "cell_type": "cellType",
                    "gene": "geneId",
                    "component": "credibleSetIndex",
                }
            )
            .join(components, on=keys)
            .join(sample_sizes, on=["cellType", "geneId"], how="left")
            .select(
                cls.build_study_id(f.col("cellType"), f.col("geneId")).alias(
                    "studyId"
                ),
                *keys,
                f.regexp_replace(f.col("variant_id"), "^chr", "").alias("variantId"),
                normalize_chromosome(f.col("chr")).alias("chromosome"),
                "locusStart",
                "locusEnd",
                f.lit(StudyType.SCEQTL.value).alias("studyType"),
                f.lit(FinemappingMethod.SUSIE.value).alias("fineMappingMethod"),
                "credibleSetLogBayesFactor",
                "purityMeanR2",
                "purityMinR2",
                *split_pvalue_column(f.col("pval_nominal")),
                f.col("beta").alias("reportedBeta"),
                f.col("standard_error").alias("reportedStandardError"),
                effect_allele_frequency.alias("effectAlleleFrequency"),
                f.col("nSamples").alias("sampleSize"),
                f.col("component_pip").alias("posteriorProbability"),
                f.col("component_logbf").alias("logBayesFactor"),
                f.col("conditional_beta").alias("conditionalBeta"),
                f.col("conditional_sd").alias("conditionalStandardError"),
                f.when(
                    effect_allele_frequency.isNotNull(),
                    f.array(
                        f.lit(CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value)
                    ),
                )
                .otherwise(f.array())
                .alias("qualityControls"),
            )
        )
        return CredibleSet.from_members(
            members,
            ["studyId", "credibleSetIndex"],
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
