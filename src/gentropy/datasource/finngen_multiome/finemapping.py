"""Datasource ingestion: FinnGen multiome single-cell eQTL fine-mapping (SuSiE) to StudyLocus."""

from __future__ import annotations

from dataclasses import dataclass

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame

from gentropy.common.session import Session
from gentropy.common.stats import split_pvalue_column
from gentropy.dataset.study_index import StudyType
from gentropy.dataset.study_locus import FinemappingMethod, StudyLocus
from gentropy.datasource.finngen.finemapping import FinnGenFinemapping


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

    @staticmethod
    def _pick_single_effect(prefix: str) -> Column:
        """Pick the per-effect value matching the credible set index.

        Args:
            prefix (str): Column prefix of the per-effect columns, e.g. `alpha` for `alpha1..alpha10`.

        Returns:
            Column: Value of the effect given by `credibleSetIndex`.
        """
        return f.element_at(
            f.array(
                *[f.col(f"{prefix}{i}").cast(t.DoubleType()) for i in range(1, 11)]
            ),
            f.col("credibleSetIndex"),
        )

    @classmethod
    def read_snp_files(
        cls: type[FinnGenMultiomeFinemapping],
        session: Session,
        snp_files: str | list[str],
    ) -> DataFrame:
        """Read credible set variants and the locus boundaries of each gene.

        The files hold every variant tested for a gene, so the minimum and maximum position are
        aggregated in the same pass that keeps the credible set variants.

        Args:
            session (Session): Session object.
            snp_files (str | list[str]): Paths to the `*.SUSIE.snp.tsv.gz` files.

        Returns:
            DataFrame: One row per variant and credible set, with locus boundaries.
        """
        raw = (
            session.spark.read.schema(FinnGenFinemapping.raw_schema)
            .option("delimiter", "\t")
            .option("header", True)
            .csv(snp_files)
            .withColumn("position", f.col("position").cast(t.IntegerType()))
            .withColumn("credibleSetIndex", f.col("cs").cast(t.IntegerType()))
            .filter(f.col("position").isNotNull())
        )
        cs_variant = f.struct(
            f.regexp_replace(f.col("v"), ":", "_").alias("variantId"),
            f.regexp_replace(f.col("chromosome"), "^chr", "").alias("chromosome"),
            f.col("position"),
            f.col("credibleSetIndex"),
            f.col("beta").cast(t.DoubleType()).alias("beta"),
            f.col("se").cast(t.DoubleType()).alias("standardError"),
            f.col("p").alias("p"),
            cls._pick_single_effect("alpha").alias("posteriorProbability"),
            cls._pick_single_effect("lbf_variable").alias("logBF"),
        )
        return (
            raw.groupBy("trait", "region")
            .agg(
                f.min("position").alias("locusStart"),
                f.max("position").alias("locusEnd"),
                # collect_list drops the nulls produced for variants outside any credible set:
                f.collect_list(f.when(f.col("credibleSetIndex") > 0, cs_variant)).alias(
                    "variants"
                ),
            )
            .select(
                "trait",
                "region",
                "locusStart",
                "locusEnd",
                f.explode("variants").alias("v"),
            )
            .select("trait", "region", "locusStart", "locusEnd", "v.*")
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
    ) -> StudyLocus:
        """Build credible sets from the FinnGen multiome SuSiE results.

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

        Returns:
            StudyLocus: Credible sets, one study per cell type and gene.
        """
        cs_variants = cls.read_snp_files(session, snp_files).join(
            cls.read_cs_summary_files(
                session,
                cs_summary_files,
                credset_lbf_threshold,
                purity_min_r2_threshold,
            ),
            on=["trait", "region", "credibleSetIndex"],
            how="inner",
        )
        cs_variants = (
            cs_variants.withColumns(
                {
                    "cellType": cls.extract_cell_type(f.col("trait")),
                    "geneId": f.col("region"),
                }
            )
            .join(
                cls.read_nominal_files(session, nominal_files),
                on=["cellType", "geneId", "variantId"],
                how="left",
            )
            .select(
                cls.build_study_id(
                    project_prefix, f.col("cellType"), f.col("geneId")
                ).alias("studyId"),
                "credibleSetIndex",
                "variantId",
                "chromosome",
                "position",
                "beta",
                "standardError",
                *split_pvalue_column(f.col("p")),
                "posteriorProbability",
                "logBF",
                "effectAlleleFrequencyFromSource",
                "credibleSetlog10BF",
                "purityMeanR2",
                "purityMinR2",
                "locusStart",
                "locusEnd",
            )
        )

        # Lead variant and locus in one aggregation, so the inputs are scanned once.
        # Ties on posterior probability are broken by variant identifier to keep the lead deterministic.
        lead_columns = [
            "variantId",
            "chromosome",
            "position",
            "beta",
            "standardError",
            "pValueMantissa",
            "pValueExponent",
            "effectAlleleFrequencyFromSource",
        ]
        credible_set_columns = [
            "credibleSetlog10BF",
            "purityMeanR2",
            "purityMinR2",
            "locusStart",
            "locusEnd",
        ]
        credible_sets = (
            cs_variants.groupBy("studyId", "credibleSetIndex")
            .agg(
                f.max_by(
                    f.struct(*lead_columns),
                    f.struct("posteriorProbability", "variantId"),
                ).alias("lead"),
                *[f.first(c).alias(c) for c in credible_set_columns],
                f.collect_list(
                    f.struct(
                        f.col("variantId"),
                        f.col("posteriorProbability"),
                        f.col("logBF"),
                        f.col("pValueMantissa"),
                        f.col("pValueExponent"),
                        f.col("beta"),
                        f.col("standardError"),
                    )
                ).alias("locus"),
            )
            .select(
                "studyId",
                "credibleSetIndex",
                "lead.*",
                *credible_set_columns,
                "locus",
            )
        )

        return StudyLocus(
            _df=credible_sets.withColumns(
                {
                    "studyType": f.lit(StudyType.SCEQTL.value),
                    "finemappingMethod": f.lit(FinemappingMethod.SUSIE.value),
                    "isTransQtl": f.lit(False),
                }
            ).withColumn(
                "studyLocusId",
                StudyLocus.assign_study_locus_id(
                    ["studyId", "variantId", "finemappingMethod"]
                ),
            ),
            _schema=StudyLocus.get_schema(),
        ).annotate_credible_sets()
