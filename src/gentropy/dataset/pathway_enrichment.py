"""Disease-pathway enrichment dataset."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass
class PathwayEnrichment(Dataset):
    """Pathways enriched among the genes associated with a disease.

    One row per disease and pathway, as produced by running gene set enrichment analysis
    over the genes ranked by their association with the disease. Gene set membership is not
    part of this dataset; it lives in the matching
    [`PathwayIndex`][gentropy.dataset.pathway_index.PathwayIndex].
    """

    @classmethod
    def get_schema(cls: type[PathwayEnrichment]) -> StructType:
        """Provide the schema for the PathwayEnrichment dataset.

        Returns:
            StructType: The schema of the PathwayEnrichment dataset.
        """
        return parse_spark_schema("pathway_enrichment.json")

    def with_corrected_fdr(self: PathwayEnrichment) -> PathwayEnrichment:
        """Fill in a missing FDR with one recomputed from the p-values.

        Enrichment results can come with an FDR that the upstream tool failed to estimate,
        sometimes for every pathway of a disease at once, which would silently remove that
        disease from any FDR filter. Where `fdr` is null, this replaces it with the
        Benjamini-Hochberg step-up value computed over the p-values of that disease:
        `q(i) = min over j >= i of p(j) * n / j`, with the pathways ordered by ascending
        p-value. Pathways with no p-value keep a null FDR and are left out of `n`.

        Returns:
            PathwayEnrichment: Dataset where `fdr` is null only if `pval` is too.
        """
        by_disease = Window.partitionBy("diseaseId")
        by_pval = by_disease.orderBy(f.col("pval").asc_nulls_last())
        n_tested = f.count("pval").over(by_disease)
        # rank() rather than row_number() so that tied p-values get the same BH value.
        raw_fdr = f.col("pval") * n_tested / f.rank().over(by_pval)
        step_up = f.min(raw_fdr).over(
            by_pval.rowsBetween(Window.currentRow, Window.unboundedFollowing)
        )
        return PathwayEnrichment(
            _df=self.df.withColumn(
                "fdr", f.coalesce(f.col("fdr"), f.least(step_up, f.lit(1.0)))
            ),
            _schema=self.get_schema(),
        )

    def enriched_pathways(self: PathwayEnrichment, fdr_threshold: float) -> DataFrame:
        """Diseases and the pathways significantly enriched among their associated genes.

        Args:
            fdr_threshold (float): Maximum FDR for a pathway to count as enriched.

        Returns:
            DataFrame: Dataframe with `diseaseId` and `pathway` columns.
        """
        return (
            self.with_corrected_fdr()
            .df.filter(f.col("fdr") < fdr_threshold)
            .select("diseaseId", "pathway")
            .distinct()
        )
