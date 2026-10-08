"""Pathway gene sets from a platform release and their over-representation among disease genes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import Window

from gentropy.common.udf import hypergeometric_survival_function

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.dataset.target_index import TargetIndex


@dataclass
class PathwayLibrary:
    """GO biological process and Reactome gene sets built from an Open Targets platform release.

    The target index only carries the terms a gene is annotated to directly, so a gene sits in
    almost none of the broader processes and pathways above them. Both ontologies are therefore
    propagated up their hierarchies (the true-path rule): a gene annotated to a term belongs to
    every ancestor of the term as well.

    - GO: biological process annotations (`aspect` "P") from `target.go`, propagated over the
      `isA` and `partOf` relations of the release's `go` table. The `ancestors` column of that
      table also follows the `regulates` relations and is not used. Annotations with an evidence
      code in `excluded_go_evidence`, by default only the electronic ones (IEA), are left out.
      Obsolete terms are dropped.
    - Reactome: `target.pathways`, propagated with the `ancestors` of the release's `reactome`
      table.

    Against the GMT library the previous pathway features were built on (Enrichr GO Biological
    Process and Reactome, 26.09 release), this reproduces the GO sets with a median Jaccard
    index of 0.92 and the Reactome sets with a median of 1.00. MSigDB Hallmark sets are not
    part of the platform and have no equivalent here.

    Attributes:
        go (DataFrame): The release's `go` table
        reactome (DataFrame): The release's `reactome` table
        excluded_go_evidence (tuple[str, ...]): GO evidence codes whose annotations are left out
    """

    go: DataFrame
    reactome: DataFrame
    excluded_go_evidence: tuple[str, ...] = ("IEA",)

    def go_term_ancestors(self: PathwayLibrary) -> DataFrame:
        """Map every biological process term to itself and each of its ancestors.

        The closure is computed on the driver: there are about 30,000 non-obsolete biological
        process terms. Alternative identifiers of a term map to the same ancestors as the term.

        Returns:
            DataFrame: `termId` and `pathwayId`, one row per term and ancestor, the term
                included
        """
        terms = (
            self.go.filter(
                (f.col("namespace") == "biological_process")
                & ~f.coalesce(f.col("isObsolete"), f.lit(False))
            )
            .select("id", "altIds", "isA", "partOf")
            .collect()
        )
        parents = {
            term.id: {*(term.isA or []), *(term.partOf or [])} for term in terms
        }
        ancestors: dict[str, set[str]] = {}

        def resolve(term_id: str) -> set[str]:
            if term_id not in ancestors:
                ancestors[term_id] = {term_id}.union(
                    *(resolve(parent) for parent in parents[term_id] if parent in parents)
                )
            return ancestors[term_id]

        rows = [
            (alias, ancestor)
            for term in terms
            for alias in (term.id, *(term.altIds or []))
            for ancestor in resolve(term.id)
        ]
        return self.go.sparkSession.createDataFrame(
            rows, "termId string, pathwayId string"
        ).distinct()

    def gene_sets(
        self: PathwayLibrary,
        target_index: TargetIndex,
        min_size: int = 5,
        max_size: int = 4000,
    ) -> DataFrame:
        """Protein-coding members of every pathway whose size falls within the limits.

        Args:
            target_index (TargetIndex): Target index, the source of the annotations
            min_size (int): Smallest number of member genes a pathway may have
            max_size (int): Largest number of member genes a pathway may have

        Returns:
            DataFrame: `pathwayId` and `geneId`, one row per pathway and member gene
        """
        genes = target_index.df.filter(f.col("biotype") == "protein_coding").select(
            f.col("id").alias("geneId"), "go", "pathways"
        )
        go_sets = (
            genes.select("geneId", f.explode("go").alias("annotation"))
            .filter(
                (f.col("annotation.aspect") == "P")
                & ~f.coalesce(
                    f.col("annotation.evidence").isin(list(self.excluded_go_evidence)),
                    f.lit(False),
                )
            )
            .select("geneId", f.col("annotation.id").alias("termId"))
            .join(self.go_term_ancestors(), "termId", "inner")
        )
        reactome_sets = genes.select(
            "geneId", f.explode("pathways.pathwayId").alias("termId")
        ).join(
            self.reactome.select(
                f.col("id").alias("termId"),
                f.explode(
                    f.array_union(
                        f.array(f.col("id")),
                        f.coalesce(f.col("ancestors"), f.array().cast("array<string>")),
                    )
                ).alias("pathwayId"),
            ),
            "termId",
            "inner",
        )
        pathway_size = f.count("geneId").over(Window.partitionBy("pathwayId"))
        return (
            go_sets.select("pathwayId", "geneId")
            .unionByName(reactome_sets.select("pathwayId", "geneId"))
            .distinct()
            .withColumn("pathwaySize", pathway_size)
            .filter(f.col("pathwaySize").between(min_size, max_size))
            .drop("pathwaySize")
        )

    @staticmethod
    def over_representation(
        gene_lists: DataFrame,
        gene_sets: DataFrame,
        min_genes: int = 25,
    ) -> DataFrame:
        """Test every pathway for over-representation among the genes of each disease.

        One-sided hypergeometric test, P(X >= k), with the genes of the library as the
        background: `N` genes belong to at least one of the gene sets, `K` of them to the
        pathway, `n` are on the disease's list and `k` of those are in the pathway. Genes of a
        list that no gene set contains are not counted. Diseases with fewer than `min_genes`
        genes in the background are not tested.

        P-values are adjusted with Benjamini-Hochberg within each disease over every pathway of
        the library. Only pairs with at least one overlapping gene are returned: the others
        have a p-value of 1, and leaving them out does not change the adjusted p-value of the
        rest, which uses the full number of pathways.

        Args:
            gene_lists (DataFrame): `diseaseId` and `geneId`, the genes of each disease
            gene_sets (DataFrame): `pathwayId` and `geneId`, as returned by `gene_sets`
            min_genes (int): Smallest number of background genes a disease needs to be tested

        Returns:
            DataFrame: `diseaseId`, `pathwayId`, `overlap` (k), `pathwaySize` (K),
                `diseaseGeneCount` (n), `backgroundSize` (N), `pValue` and `pValueAdjusted`
        """
        library = gene_sets.agg(
            f.countDistinct("geneId").alias("backgroundSize"),
            f.countDistinct("pathwayId").alias("pathwayCount"),
        )
        pathway_sizes = gene_sets.groupBy("pathwayId").agg(
            f.count("geneId").alias("pathwaySize")
        )
        disease_genes = (
            gene_lists.select("diseaseId", "geneId")
            .distinct()
            .join(gene_sets.select("geneId").distinct(), "geneId", "semi")
        )
        disease_sizes = (
            disease_genes.groupBy("diseaseId")
            .agg(f.count("geneId").alias("diseaseGeneCount"))
            .filter(f.col("diseaseGeneCount") >= min_genes)
        )
        overlaps = (
            disease_genes.join(disease_sizes, "diseaseId", "semi")
            .join(gene_sets, "geneId", "inner")
            .groupBy("diseaseId", "pathwayId")
            .agg(f.count("geneId").alias("overlap"))
        )
        by_p_value = Window.partitionBy("diseaseId").orderBy(
            f.col("pValue").asc(), f.col("pathwayId").asc()
        )
        # q(i) = min over j >= i of p(j) * m / j, with row_number() rather than rank() so
        # that positions are consecutive; tied p-values end up with the same value anyway.
        step_up = f.min(
            f.col("pValue") * f.col("pathwayCount") / f.col("pValueRank")
        ).over(by_p_value.rowsBetween(Window.currentRow, Window.unboundedFollowing))
        return (
            overlaps.join(pathway_sizes, "pathwayId", "inner")
            .join(disease_sizes, "diseaseId", "inner")
            .crossJoin(library)
            .withColumn(
                "pValue",
                hypergeometric_survival_function(
                    "overlap", "backgroundSize", "pathwaySize", "diseaseGeneCount"
                ),
            )
            .withColumn("pValueRank", f.row_number().over(by_p_value))
            .withColumn("pValueAdjusted", f.least(step_up, f.lit(1.0)))
            .select(
                "diseaseId",
                "pathwayId",
                "overlap",
                "pathwaySize",
                "diseaseGeneCount",
                "backgroundSize",
                "pValue",
                "pValueAdjusted",
            )
        )
