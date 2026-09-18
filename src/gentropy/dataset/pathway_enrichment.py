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

    from gentropy.common.session import Session


@dataclass
class PathwayEnrichment(Dataset):
    """Pathways enriched among the genes associated with a disease.

    One row per disease and pathway, as produced by running gene set enrichment analysis over
    the genes ranked by their association with the disease. Two rows of the catalogue used so
    far:

    | diseaseId        | pathwayFromSourceName                                                                    | source  | normalisedEnrichmentScore | pValue   | pValueAdjusted |
    | ---------------- | -------------------------------------------------------------------------- | ------- | ------------------------- | -------- | -------------- |
    | `MONDO_0005148`  | `Positive Regulation of DNA-templated Transcription (GO:0045893) [GO BP]`   | `GO BP` | 6.355                     | 2.08e-10 | 8.34e-08       |
    | `MONDO_0005148`  | `Regulation of Transcription by RNA Polymerase II (GO:0006357) [GO BP]`     | `GO BP` | 6.274                     | 3.52e-10 | 1.03e-07       |

    `pValueAdjusted` is the Benjamini-Hochberg adjusted p-value of the enrichment test, not a
    false discovery rate of the whole set. Gene set membership is not part of this dataset; it
    lives in the matching
    [`PathwayIndex`][gentropy.dataset.pathway_index.PathwayIndex], joined on
    `pathwayFromSourceName`.
    """

    @classmethod
    def get_schema(cls: type[PathwayEnrichment]) -> StructType:
        """Provide the schema for the PathwayEnrichment dataset.

        Returns:
            StructType: The schema of the PathwayEnrichment dataset.
        """
        return parse_spark_schema("pathway_enrichment.json")

    @classmethod
    def from_gsea_catalogue(
        cls: type[PathwayEnrichment], session: Session, path: str
    ) -> PathwayEnrichment:
        """Read a gene set enrichment catalogue and harmonise its column names.

        The catalogue this was built against carries the column names of the tool that produced
        it - `nes`, `pval`, `fdr` - alongside columns the dataset does not keep, such as the
        unnormalised enrichment score, the Sidak corrected p-value and the gene set size. It is
        partitioned by `diseaseId`, so it is read without recursive file lookup, which would
        stop Spark from reading that partition column back.

        Args:
            session (Session): Spark session
            path (str): Path to the enrichment results, partitioned by `diseaseId`

        Returns:
            PathwayEnrichment: Disease-pathway enrichment dataset
        """
        return cls(
            _df=session.spark.read.parquet(path).select(
                f.col("diseaseId").cast("string").alias("diseaseId"),
                f.col("pathway").cast("string").alias("pathwayFromSourceName"),
                f.col("source").cast("string").alias("source"),
                f.col("nes").cast("double").alias("normalisedEnrichmentScore"),
                f.col("pval").cast("double").alias("pValue"),
                f.col("fdr").cast("double").alias("pValueAdjusted"),
            ),
            _schema=cls.get_schema(),
        )

    def with_recomputed_adjusted_p_value(
        self: PathwayEnrichment, skip_infinite_enrichment: bool = True
    ) -> PathwayEnrichment:
        """Fill in a missing adjusted p-value with one recomputed from the p-values.

        Enrichment results can come with an adjusted p-value the upstream tool failed to
        estimate, sometimes for every pathway of a disease at once, which would silently remove
        that disease from any significance filter. Where `pValueAdjusted` is null, this
        replaces it with the Benjamini-Hochberg step-up value computed over the p-values of
        that disease, `q(i) = min over j >= i of p(j) * n / j`, with the pathways ordered by
        ascending p-value and `i` their position in that order. A published `pValueAdjusted` is
        kept as it is, and a pathway with no p-value keeps a null adjusted p-value and is left
        out of `n`.

        The recomputed value is adjusted over the rows the dataset holds, which is not
        necessarily the set the upstream tool adjusted over. In the catalogue this was developed
        against only positively enriched pathways are kept, a median of 4,107 rows per disease
        against 8,217 pathways actually tested, so the recomputed value is more conservative
        than and not comparable with the published one - on diseases that have both, the count
        of pathways below 0.05 ranges from 1% to 100% of the published count. It exists to
        rescue the 249 of 3,766 diseases whose adjusted p-value is null throughout, and should
        give way to a fixed upstream column.

        Args:
            skip_infinite_enrichment (bool): Whether to leave the adjusted p-value null for
                pathways with a non-finite normalised enrichment score. Those come with a
                p-value of exactly zero, which any recomputation would turn into the most
                significant adjusted p-value of the disease, so they are skipped by default.

        Returns:
            PathwayEnrichment: Dataset where `pValueAdjusted` is null only if it could not be
                recomputed.
        """
        correctable = f.col("pValue").isNotNull()
        if skip_infinite_enrichment:
            correctable = correctable & (
                f.col("normalisedEnrichmentScore").isNull()
                | ~f.isnan(f.col("normalisedEnrichmentScore"))
                & (f.abs(f.col("normalisedEnrichmentScore")) != f.lit(float("inf")))
            )
        # Rank the correctable rows of a disease by ascending p-value, keeping the rest out of
        # the ordering so that they neither take a position nor count towards `n`.
        by_disease = Window.partitionBy("diseaseId", "correctable")
        by_p_value = by_disease.orderBy(f.col("pValue").asc(), f.col("pathwayFromSourceName").asc())
        # row_number() rather than rank(): Benjamini-Hochberg needs consecutive positions, and
        # the step-up minimum gives tied p-values the same value anyway.
        raw = f.col("pValue") * f.count("pValue").over(by_disease) / f.row_number().over(
            by_p_value
        )
        step_up = f.min(raw).over(
            by_p_value.rowsBetween(Window.currentRow, Window.unboundedFollowing)
        )
        return PathwayEnrichment(
            _df=(
                self.df.withColumn("correctable", correctable)
                .withColumn(
                    "pValueAdjusted",
                    f.coalesce(
                        f.col("pValueAdjusted"),
                        f.when(f.col("correctable"), f.least(step_up, f.lit(1.0))),
                    ),
                )
                .drop("correctable")
            ),
            _schema=self.get_schema(),
        )

    def enriched_pathways(
        self: PathwayEnrichment,
        p_value_adjusted_threshold: float,
        recompute_missing_adjusted_p_value: bool = True,
    ) -> DataFrame:
        """Diseases and the pathways significantly enriched among their associated genes.

        Args:
            p_value_adjusted_threshold (float): Maximum adjusted p-value for a pathway to count
                as enriched.
            recompute_missing_adjusted_p_value (bool): Whether to recompute an adjusted p-value
                the upstream tool left null, with the caveats described in
                [`with_recomputed_adjusted_p_value`][gentropy.dataset.pathway_enrichment.PathwayEnrichment.with_recomputed_adjusted_p_value].

        Returns:
            DataFrame: Dataframe with `diseaseId` and `pathwayFromSourceName` columns.
        """
        enrichment = (
            self.with_recomputed_adjusted_p_value()
            if recompute_missing_adjusted_p_value
            else self
        )
        return (
            enrichment.df.filter(
                f.col("pValueAdjusted") < p_value_adjusted_threshold
            )
            .select("diseaseId", "pathwayFromSourceName")
            .distinct()
        )

    def tested_pathways(self: PathwayEnrichment) -> DataFrame:
        """Pathways enrichment was tested for, whatever the outcome.

        A pathway is taken as tested if any disease has a p-value or an adjusted p-value for
        it. This is the denominator of the pathway enrichment features: a pathway that was
        never tested must not count against a gene that belongs to it.

        Returns:
            DataFrame: Dataframe with a single `pathwayFromSourceName` column.
        """
        return (
            self.df.filter(
                f.col("pValue").isNotNull() | f.col("pValueAdjusted").isNotNull()
            )
            .select("pathwayFromSourceName")
            .distinct()
        )
