"""Enrichment of disease genes among the genes specifically expressed in each biosample."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.udf import chi2_survival_function

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.dataset.target_index import TargetIndex


@dataclass
class ExpressionSpecificity:
    """Expression specificity of every gene in every biosample, from an Open Targets platform release.

    Wraps the release's `baseline_expression` table: one row per gene and biosample of a
    datasource, with the gene's `specificity_score` there. A biosample is a tissue
    (`tissueBiosampleId`, UBERON), a cell type (`celltypeBiosampleId`, CL), or a cell type
    within a tissue, where both are set. Rows without a specificity score, which is every row
    of the proteomics datasource, are not used.

    Attributes:
        baseline_expression (DataFrame): The release's `baseline_expression` table
    """

    baseline_expression: DataFrame

    def specificity(self: ExpressionSpecificity) -> DataFrame:
        """Specificity score of each gene in each biosample of each datasource.

        `biosampleId` is the identifier a study would use for the biosample: the tissue or the
        cell type when only one of them is set, and null for a cell type within a tissue.

        Returns:
            DataFrame: `datasourceId`, `tissueBiosampleId`, `celltypeBiosampleId`,
                `biosampleId`, `geneId` and `specificityScore`
        """
        return self.baseline_expression.filter(
            f.col("specificity_score").isNotNull()
        ).select(
            "datasourceId",
            "tissueBiosampleId",
            "celltypeBiosampleId",
            f.when(
                f.col("tissueBiosampleId").isNull()
                | f.col("celltypeBiosampleId").isNull(),
                f.coalesce("celltypeBiosampleId", "tissueBiosampleId"),
            ).alias("biosampleId"),
            f.col("targetId").alias("geneId"),
            f.col("specificity_score").alias("specificityScore"),
        )

    def enrichment(
        self: ExpressionSpecificity,
        gene_lists: DataFrame,
        target_index: TargetIndex,
        min_genes: int = 25,
    ) -> DataFrame:
        """Test every biosample for disease genes being more specifically expressed there, leaving one chromosome out at a time.

        For each disease and each biosample of a datasource this is the logistic regression of
        whether a gene is on the disease's list on the gene's specificity score in the
        biosample, tested one-sided for a positive slope. The genes are the protein-coding
        genes with a specificity score in the datasource; a gene without a score in a
        biosample of its datasource counts as 0. Diseases with fewer than `min_genes` of those
        genes are not tested in that datasource.

        The test is the score test of the slope, which needs no model fit. With `n` genes, `m`
        of them on the list, specificity scores `x` and `S` the sum of the scores of the listed
        genes:

            Z = (S - m * mean(x)) / sqrt(m / n * (1 - m / n) * sum((x - mean(x)) ** 2))

        A negative slope is not evidence for the biosample, so the p-value is P(N(0, 1) > Z)
        when Z is positive and 1 otherwise. Biosamples where every gene has the same score
        cannot be tested and are left out.

        Each disease is tested once per held-out chromosome, on its list without the genes of
        that chromosome, so that a locus can be scored without its own genes or those of any
        other credible set nearby. Every chromosome of `gene_lists` is held out in turn. Only
        `m` and `S` change when a chromosome is held out, so they are summed once per
        chromosome and each held-out value is the total minus that chromosome's part.

        P-values are adjusted with Benjamini-Hochberg within each disease and held-out
        chromosome over every biosample of every datasource it was tested in.

        Args:
            gene_lists (DataFrame): `diseaseId`, `chromosome` and `geneId`, the genes of each
                disease with the chromosome they are on
            target_index (TargetIndex): Target index, used to find the protein-coding genes
            min_genes (int): Smallest number of genes of a datasource a disease needs to be
                tested there

        Returns:
            DataFrame: `diseaseId`, `heldOutChromosome`, `datasourceId`, `tissueBiosampleId`,
                `celltypeBiosampleId`, `biosampleId`, `diseaseGeneCount` (m), `geneCount` (n),
                `zScore`, `pValue` and `pValueAdjusted`
        """
        biosample_cols = [
            "datasourceId",
            "tissueBiosampleId",
            "celltypeBiosampleId",
            "biosampleId",
        ]
        specificity = (
            self.specificity()
            .join(
                target_index.df.filter(f.col("biotype") == "protein_coding").select(
                    f.col("id").alias("geneId")
                ),
                "geneId",
                "semi",
            )
            # One non-null key per biosample of a datasource, for joins and ordering.
            .withColumn(
                "biosampleKey",
                f.concat_ws(
                    "|",
                    "datasourceId",
                    f.coalesce("tissueBiosampleId", f.lit("")),
                    f.coalesce("celltypeBiosampleId", f.lit("")),
                ),
            )
        )
        datasource_genes = specificity.select("datasourceId", "geneId").distinct()
        biosamples = (
            specificity.groupBy("biosampleKey", *biosample_cols)
            .agg(
                f.sum("specificityScore").alias("scoreSum"),
                f.sum(f.col("specificityScore") ** 2).alias("scoreSquareSum"),
            )
            .join(
                datasource_genes.groupBy("datasourceId").agg(
                    f.count("geneId").alias("geneCount")
                ),
                "datasourceId",
            )
        )
        disease_genes = (
            gene_lists.select("diseaseId", "chromosome", "geneId")
            .distinct()
            .join(datasource_genes, "geneId")
        )
        held_out_chromosomes = gene_lists.select(
            f.col("chromosome").alias("heldOutChromosome")
        ).distinct()
        chromosome_sizes = disease_genes.groupBy(
            "diseaseId", "datasourceId", "chromosome"
        ).agg(f.count("geneId").alias("chromosomeGeneCount"))
        disease_sizes = (
            chromosome_sizes.groupBy("diseaseId", "datasourceId")
            .agg(f.sum("chromosomeGeneCount").alias("totalGeneCount"))
            .crossJoin(held_out_chromosomes)
            .join(
                chromosome_sizes.withColumnRenamed("chromosome", "heldOutChromosome"),
                ["diseaseId", "datasourceId", "heldOutChromosome"],
                "left",
            )
            .select(
                "diseaseId",
                "datasourceId",
                "heldOutChromosome",
                (
                    f.col("totalGeneCount")
                    - f.coalesce(f.col("chromosomeGeneCount"), f.lit(0))
                ).alias("diseaseGeneCount"),
            )
            .filter(f.col("diseaseGeneCount") >= min_genes)
        )
        chromosome_score_sums = (
            disease_genes.join(disease_sizes, ["diseaseId", "datasourceId"], "semi")
            .join(specificity, ["datasourceId", "geneId"])
            .groupBy("diseaseId", "biosampleKey", "chromosome")
            .agg(f.sum("specificityScore").alias("chromosomeScoreSum"))
        )
        total_score_sums = chromosome_score_sums.groupBy(
            "diseaseId", "biosampleKey"
        ).agg(f.sum("chromosomeScoreSum").alias("totalScoreSum"))
        # Every biosample of a datasource is tested, also those where no listed gene scores.
        tests = (
            disease_sizes.join(biosamples, "datasourceId")
            .join(total_score_sums, ["diseaseId", "biosampleKey"], "left")
            .join(
                chromosome_score_sums.withColumnRenamed(
                    "chromosome", "heldOutChromosome"
                ),
                ["diseaseId", "biosampleKey", "heldOutChromosome"],
                "left",
            )
            .withColumn(
                "diseaseScoreSum",
                f.coalesce(f.col("totalScoreSum"), f.lit(0.0))
                - f.coalesce(f.col("chromosomeScoreSum"), f.lit(0.0)),
            )
        )

        mean_score = f.col("scoreSum") / f.col("geneCount")
        listed_fraction = f.col("diseaseGeneCount") / f.col("geneCount")
        score_variance = (
            listed_fraction
            * (1 - listed_fraction)
            * (f.col("scoreSquareSum") - f.col("scoreSum") * mean_score)
        )
        z_score = (
            f.col("diseaseScoreSum") - f.col("diseaseGeneCount") * mean_score
        ) / f.sqrt(score_variance)

        by_disease = Window.partitionBy("diseaseId", "heldOutChromosome")
        by_p_value = by_disease.orderBy(f.col("pValue").asc(), f.col("biosampleKey"))
        # q(i) = min over j >= i of p(j) * m / j, as in `PathwayLibrary.over_representation`.
        step_up = f.min(
            f.col("pValue") * f.col("testCount") / f.col("pValueRank")
        ).over(by_p_value.rowsBetween(Window.currentRow, Window.unboundedFollowing))
        return (
            tests.filter(score_variance > 0)
            .withColumn("zScore", z_score)
            .withColumn(
                "pValue",
                f.when(
                    f.col("zScore") > 0,
                    chi2_survival_function(f.col("zScore") ** 2) / 2,
                ).otherwise(f.lit(1.0)),
            )
            .withColumn("testCount", f.count("*").over(by_disease))
            .withColumn("pValueRank", f.row_number().over(by_p_value))
            .withColumn("pValueAdjusted", f.least(step_up, f.lit(1.0)))
            .select(
                "diseaseId",
                "heldOutChromosome",
                *biosample_cols,
                "diseaseGeneCount",
                "geneCount",
                "zScore",
                "pValue",
                "pValueAdjusted",
            )
        )
