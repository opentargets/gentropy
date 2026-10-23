"""Datasource ingestion: FinnGen multiome single-cell eQTL fine-mapping (SuSiE) to credible sets."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame, Window

from gentropy.common.processing import normalize_chromosome
from gentropy.common.session import Session
from gentropy.common.stats import region_log_bayes_factors, split_pvalue_column
from gentropy.dataset.credible_set import (
    CredibleSet,
    CredibleSetVariant,
    CredibleSetVariantQualityCheck,
)
from gentropy.dataset.study_index import StudyType
from gentropy.dataset.study_locus import FinemappingMethod
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping, _effect


@dataclass
class FinnGenMultiomeFinemapping:
    """SuSiE fine-mapping of single-cell cis-eQTLs from the FinnGen multiome atlas.

    The fine-mapping files follow the FinnGen SuSiE layout (`*.SUSIE.snp.tsv.gz` and
    `*.SUSIE.cred.tsv.gz`), with two differences from the FinnGen GWAS release:

    - `region` holds the Ensembl gene identifier rather than a `chr:start-end` window, so the
      locus boundaries are taken from the positions of all variants tested for that gene.
    - `maf` is the minor allele frequency. The effect allele frequency is taken from the
      cis-nominal summary statistics (`AF_Allele2`) instead.

    One study is defined per cell type and gene. Credible sets are the 95% sets reported in the
    `cs` column. Credible sets are kept if they are the first in the region or their log10 Bayes
    factor exceeds `credset_lbf_threshold`, and if their minimum pairwise r2 is at least
    `purity_min_r2_threshold`.

    The files are block gzipped with tabix indices, so they are read with a session using
    `use_enhanced_bgzip_codec`, which splits them across tasks.

    An example row of the SuSiE variant files (`*.SUSIE.snp.tsv.gz`, one row per variant tested
    for a gene, with the per-effect `alpha1..10`, `mean1..10`, `sd1..10` and `lbf_variable1..10`
    columns not shown):

    | trait | region | v | chromosome | position | allele1 | allele2 | maf | beta | se | p | cs |
    |---|---|---|---|---|---|---|---|---|---|---|---|
    | integrated_gex_batch1_5.fgid.qc.predicted.celltype.l1.B.mean.inv.SAIGE | ENSG00000015475 | 22:17774372:T:G | chr22 | 17774372 | T | G | 0.37647 | 0.1694 | 0.014516 | 2.3306e-31 | 1 |

    An example row of the credible set summaries (`*.SUSIE.cred.tsv.gz`):

    | trait | region | cs | cs_log10bf | cs_avg_r2 | cs_min_r2 | low_purity | cs_size |
    |---|---|---|---|---|---|---|---|
    | integrated_gex_batch1_5.fgid.qc.predicted.celltype.l1.B.mean.inv.SAIGE | ENSG00000015475 | 1 | 27.148 | 1 | 1 | FALSE | 1 |

    An example row of the cis-nominal summary statistics (`*.cis_nominal.tsv.gz`), of which only
    the alternative allele frequency `AF_Allele2` is used:

    | #CHR | POS | cell_type | phenotype_id | MarkerID | AF_Allele2 | BETA | SE | p.value |
    |---|---|---|---|---|---|---|---|---|
    | chr22 | 16774862 | predicted.celltype.l1.B | ENSG00000015475 | chr22_16774862_A_G | 0.30818 | -0.00031305 | 0.015152 | 0.98352 |
    """

    cs_summary_schema: t.StructType = t.StructType(
        [
            t.StructField("trait", t.StringType(), True),
            t.StructField("region", t.StringType(), True),
            t.StructField("cs", t.IntegerType(), True),
            t.StructField("cs_log10bf", t.DoubleType(), True),
            t.StructField("cs_avg_r2", t.DoubleType(), True),
            t.StructField("cs_min_r2", t.DoubleType(), True),
            t.StructField("low_purity", t.StringType(), True),
            t.StructField("cs_size", t.IntegerType(), True),
        ]
    )

    nominal_schema: t.StructType = t.StructType(
        [
            t.StructField("chromosome", t.StringType(), True),
            t.StructField("position", t.IntegerType(), True),
            t.StructField("cell_type", t.StringType(), True),
            t.StructField("phenotype_id", t.StringType(), True),
            t.StructField("MarkerID", t.StringType(), True),
            t.StructField("AF_Allele2", t.DoubleType(), True),
            t.StructField("BETA", t.DoubleType(), True),
            t.StructField("SE", t.DoubleType(), True),
            t.StructField("pvalue", t.StringType(), True),
        ]
    )

    @staticmethod
    def extract_cell_type(col: Column) -> Column:
        """Extract the Azimuth cell type with its annotation level from a trait or cell type label.

        Args:
            col (Column): Trait name from the fine-mapping files or cell type from the nominal files.

        Returns:
            Column: Cell type as `<level>.<cell type>`, e.g. `l2.CD4_Naive`.

        Examples:
            >>> df = spark.createDataFrame(
            ...     [
            ...         ("integrated_gex_batch1_5.fgid.qc.predicted.celltype.l1.B.mean.inv.SAIGE",),
            ...         ("predicted.celltype.l2.NK_CD56bright",),
            ...     ],
            ...     ["label"],
            ... )
            >>> df.select(FinnGenMultiomeFinemapping.extract_cell_type(f.col("label")).alias("cellType")).show()
            +----------------+
            |        cellType|
            +----------------+
            |            l1.B|
            |l2.NK_CD56bright|
            +----------------+
            <BLANKLINE>
        """
        return f.regexp_extract(col, r"predicted\.celltype\.(l[0-9]+\.[^.]+)", 1)

    @staticmethod
    def build_study_id(
        project_prefix: str, cell_type: Column, gene_id: Column
    ) -> Column:
        """Build the study identifier from the cell type and gene.

        Args:
            project_prefix (str): Project prefix, e.g. `FINNGEN_MULTIOME_V1`.
            cell_type (Column): Cell type as `<level>.<cell type>`.
            gene_id (Column): Ensembl gene identifier.

        Returns:
            Column: Study identifier.

        Examples:
            >>> df = spark.createDataFrame([("l2.CD4_Naive", "ENSG00000015475")], ["cellType", "geneId"])
            >>> df.select(
            ...     FinnGenMultiomeFinemapping.build_study_id(
            ...         "FINNGEN_MULTIOME_V1", f.col("cellType"), f.col("geneId")
            ...     ).alias("studyId")
            ... ).show(truncate=False)
            +---------------------------------------------------+
            |studyId                                            |
            +---------------------------------------------------+
            |FINNGEN_MULTIOME_V1_ge_l2_CD4_Naive_ENSG00000015475|
            +---------------------------------------------------+
            <BLANKLINE>
        """
        return f.concat_ws(
            "_",
            f.lit(project_prefix),
            f.lit("ge"),
            f.regexp_replace(cell_type, r"\.", "_"),
            gene_id,
        )

    @classmethod
    def read_snp_files(
        cls: type[FinnGenMultiomeFinemapping],
        session: Session,
        snp_files: str | list[str],
    ) -> DataFrame:
        """Read the SuSiE variant files: every variant tested for a gene, one row each.

        Args:
            session (Session): Session object.
            snp_files (str | list[str]): Paths to the `*.SUSIE.snp.tsv.gz` files.

        Returns:
            DataFrame: The variants, keyed by `trait` and `region` (the gene).
        """
        return (
            session.spark.read.schema(FinnGenFinemapping.raw_schema)
            .option("delimiter", "\t")
            .option("header", True)
            .csv(snp_files)
            .withColumn("position", f.col("position").cast(t.IntegerType()))
            .filter(f.col("position").isNotNull())
        )

    @classmethod
    def read_cs_summary_files(
        cls: type[FinnGenMultiomeFinemapping],
        session: Session,
        cs_summary_files: str | list[str],
        credset_lbf_threshold: float,
        purity_min_r2_threshold: float,
    ) -> DataFrame:
        """Read credible set summaries and keep the credible sets passing the filters.

        Args:
            session (Session): Session object.
            cs_summary_files (str | list[str]): Paths to the `*.SUSIE.cred.tsv.gz` files.
            credset_lbf_threshold (float): Minimum log10 Bayes factor for credible sets other than the first.
            purity_min_r2_threshold (float): Minimum pairwise r2 within the credible set.

        Returns:
            DataFrame: Credible set summaries.
        """
        return (
            session.spark.read.schema(cls.cs_summary_schema)
            .option("delimiter", "\t")
            .option("header", True)
            .csv(cs_summary_files)
            .select(
                "trait",
                "region",
                f.col("cs").alias("credibleSetIndex"),
                f.col("cs_log10bf").alias("credibleSetlog10BF"),
                f.col("cs_avg_r2").alias("purityMeanR2"),
                f.col("cs_min_r2").alias("purityMinR2"),
            )
            .filter(
                (f.col("credibleSetlog10BF") > credset_lbf_threshold)
                | (f.col("credibleSetIndex") == 1)
            )
            .filter(f.col("purityMinR2") >= purity_min_r2_threshold)
        )

    @classmethod
    def read_nominal_files(
        cls: type[FinnGenMultiomeFinemapping],
        session: Session,
        nominal_files: str | list[str],
    ) -> DataFrame:
        """Read effect allele frequencies from the cis-nominal summary statistics.

        Args:
            session (Session): Session object.
            nominal_files (str | list[str]): Paths to the `*.cis_nominal.tsv.gz` files.

        Returns:
            DataFrame: Effect allele frequency per cell type, gene and variant.
        """
        return (
            session.spark.read.schema(cls.nominal_schema)
            .option("delimiter", "\t")
            .option("header", True)
            .csv(nominal_files)
            .select(
                cls.extract_cell_type(f.col("cell_type")).alias("cellType"),
                f.col("phenotype_id").alias("geneId"),
                f.regexp_replace(f.col("MarkerID"), "^chr", "").alias("variantId"),
                f.col("AF_Allele2")
                .cast(t.FloatType())
                .alias("effectAlleleFrequencyFromSource"),
            )
        )

    @classmethod
    def from_source(
        cls: type[FinnGenMultiomeFinemapping],
        session: Session,
        snp_files: str | list[str],
        cs_summary_files: str | list[str],
        nominal_files: str | list[str],
        project_prefix: str,
        credset_lbf_threshold: float = 0.8685889638065036,
        purity_min_r2_threshold: float = 0.25,
        lead_pvalue_threshold: float | None = None,
    ) -> tuple[CredibleSet, CredibleSetVariant]:
        """Build credible sets and their variants from the FinnGen multiome SuSiE results.

        The columns follow the FinnGen GWAS ingestion (`FinnGenFinemapping.from_finngen_susie_finemapping`),
        whose SuSiE layout these files share:

        - `reportedBeta`, `reportedStandardError`, `pValueMantissa`, `pValueExponent`: the
          marginal `beta`, `se` and `p` of the alternative allele.
        - `posteriorProbability`, `logBayesFactor`: `alpha<cs>` and `lbf_variable<cs>` of the
          credible set's single effect.
        - `conditionalBeta`, `conditionalStandardError`: the posterior mean and standard deviation
          of the effect given that the variant is causal, recovered from FinnGen's alpha-weighted
          `mean<cs>` and `sd<cs>` as `mean<cs> / alpha<cs>` and
          `sqrt((sd<cs>^2 + mean<cs>^2) / alpha<cs> - (mean<cs> / alpha<cs>)^2)`.
        - `r2Overall`: `lead_r2`.
        - `effectAlleleFrequency`: `AF_Allele2` of the cis-nominal files, flagged `EAF_FROM_SOURCE`.
        - Credible set `logBayesFactor`: the Bayes factor of the single effect over every variant
          tested for the gene (see `gentropy.common.stats.region_log_bayes_factors`).
        - `locusStart`, `locusEnd`: the positions of the first and last variant tested for the gene.

        Args:
            session (Session): Session object.
            snp_files (str | list[str]): Paths to the `*.SUSIE.snp.tsv.gz` files.
            cs_summary_files (str | list[str]): Paths to the `*.SUSIE.cred.tsv.gz` files.
            nominal_files (str | list[str]): Paths to the `*.cis_nominal.tsv.gz` files.
            project_prefix (str): Prefix for the study identifiers.
            credset_lbf_threshold (float): Minimum log10 Bayes factor for credible sets other than the first.
                Default 0.8685889638065036 == np.log10(np.exp(2)), as in the FinnGen GWAS ingestion.
            purity_min_r2_threshold (float): Minimum pairwise r2 within the credible set. Default 0.25,
                which is also the cut-off behind FinnGen's `low_purity` flag.
            lead_pvalue_threshold (float | None): Drop the credible sets whose lead p-value is above it.
                Defaults to None, which keeps every credible set.

        Returns:
            tuple[CredibleSet, CredibleSetVariant]: Credible sets, one study per cell type and gene, and their variants.
        """
        snps = cls.read_snp_files(session, snp_files)
        credible_sets = cls.read_cs_summary_files(
            session, cs_summary_files, credset_lbf_threshold, purity_min_r2_threshold
        )
        keys = ["trait", "region", "credibleSetIndex"]
        gene = Window.partitionBy("trait", "region")
        # Every variant tested for the gene counts, for the Bayes factor and the locus bounds.
        # Both come from the same aggregation, so the variant files are scanned once for them:
        region_statistics = region_log_bayes_factors(
            snps.select(
                "trait",
                "region",
                "position",
                *[f"lbf_variable{i}" for i in range(1, 11)],
            )
            .join(credible_sets.select(*keys), on=["trait", "region"])
            .withColumns(
                {
                    "locusStart": f.min("position").over(gene),
                    "locusEnd": f.max("position").over(gene),
                }
            ),
            [*keys, "locusStart", "locusEnd"],
            _effect("lbf_variable").cast(t.DoubleType()),
        )
        # Read twice, to select the allele frequencies and to join them: persist, so the
        # SuSiE outputs are read once.
        members = (
            snps.withColumn("credibleSetIndex", f.col("cs").cast(t.IntegerType()))
            .filter(f.col("credibleSetIndex") > 0)
            .join(credible_sets, on=keys)
            .join(region_statistics, on=keys)
            .withColumns(
                {
                    "cellType": cls.extract_cell_type(f.col("trait")),
                    "geneId": f.col("region"),
                    "variantId": f.regexp_replace(f.col("v"), ":", "_"),
                }
            )
            .persist()
        )
        allele_frequencies = cls.read_nominal_files(session, nominal_files).join(
            members.select("cellType", "geneId", "variantId").distinct(),
            on=["cellType", "geneId", "variantId"],
        )
        effect_allele_frequency = f.col("effectAlleleFrequencyFromSource")
        alpha = _effect("alpha").cast(t.DoubleType())
        mean = _effect("mean").cast(t.DoubleType())
        sd = _effect("sd").cast(t.DoubleType())
        # FinnGen reports the effect weighted by alpha; dividing by it gives the conditional
        # moments. Clamped at 0, as the variance can come out slightly negative in floating point:
        conditional_beta = f.when(alpha > 0, mean / alpha)
        conditional_variance = (sd**2 + mean**2) / alpha - conditional_beta**2
        members = members.join(
            allele_frequencies, on=["cellType", "geneId", "variantId"], how="left"
        ).select(
            cls.build_study_id(
                project_prefix, f.col("cellType"), f.col("geneId")
            ).alias("studyId"),
            "credibleSetIndex",
            "variantId",
            normalize_chromosome(f.col("chromosome")),
            "locusStart",
            "locusEnd",
            f.lit(StudyType.SCEQTL.value).alias("studyType"),
            f.lit(FinemappingMethod.SUSIE.value).alias("fineMappingMethod"),
            "credibleSetLogBayesFactor",
            "purityMeanR2",
            "purityMinR2",
            *split_pvalue_column(f.col("p").cast(t.DoubleType())),
            f.col("beta").cast(t.DoubleType()).alias("reportedBeta"),
            f.col("se").cast(t.DoubleType()).alias("reportedStandardError"),
            effect_allele_frequency.alias("effectAlleleFrequency"),
            alpha.alias("posteriorProbability"),
            _effect("lbf_variable").cast(t.DoubleType()).alias("logBayesFactor"),
            conditional_beta.alias("conditionalBeta"),
            f.when(
                alpha > 0, f.sqrt(f.greatest(conditional_variance, f.lit(0.0)))
            ).alias("conditionalStandardError"),
            f.col("lead_r2").cast(t.FloatType()).alias("r2Overall"),
            f.when(
                effect_allele_frequency.isNotNull(),
                f.array(f.lit(CredibleSetVariantQualityCheck.EAF_FROM_SOURCE.value)),
            )
            .otherwise(f.array())
            .alias("qualityControls"),
        )
        return CredibleSet.from_members(
            members,
            ["studyId", "credibleSetIndex"],
            lead_pvalue_threshold=lead_pvalue_threshold,
        )
