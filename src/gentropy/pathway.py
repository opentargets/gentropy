"""Step to ingest a pathway library and its disease-pathway enrichment results."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.pathway_enrichment import PathwayEnrichment
from gentropy.dataset.pathway_index import PathwayIndex
from gentropy.dataset.target_index import TargetIndex


class PathwayIngestionStep:
    """Ingest a pathway library and its enrichment results into harmonised Parquet datasets.

    The library arrives as a gene matrix transposed (GMT) file and the enrichment results with
    the column names of the tool that produced them. Both are harmonised here, once, so that
    every step downstream reads a validated dataset: the names and identifiers the source gave
    each pathway are kept as they are, and the pathway identifiers and gene symbols are
    resolved against a target index release.
    """

    def __init__(
        self,
        session: Session,
        pathway_library_path: str,
        pathway_enrichment_source_path: str,
        target_index_path: str,
        pathway_index_path: str,
        pathway_enrichment_path: str,
        exclude_unmapped_pathways: bool = False,
        recompute_missing_adjusted_p_value: bool = True,
    ) -> None:
        """Run the pathway ingestion step.

        Args:
            session (Session): Session object.
            pathway_library_path (str): Path to the pathway library, as a GMT file.
            pathway_enrichment_source_path (str): Path to the enrichment results of the same
                library, partitioned by `diseaseId`.
            target_index_path (str): Path to the target index the identifiers are resolved
                against.
            pathway_index_path (str): Output path of the harmonised `PathwayIndex` dataset.
            pathway_enrichment_path (str): Output path of the harmonised `PathwayEnrichment`
                dataset.
            exclude_unmapped_pathways (bool): Whether to drop the pathways whose source
                identifier does not resolve against the target index release. Off by default:
                the pathway enrichment features divide by the number of pathways a gene belongs
                to, so dropping pathways moves every score.
            recompute_missing_adjusted_p_value (bool): Whether to fill in the adjusted p-values
                the upstream tool left null with Benjamini-Hochberg values recomputed over the
                rows of each disease. On by default, because otherwise the diseases whose
                adjusted p-value failed throughout have no enriched pathway at all. The
                recomputed values are adjusted over the stored rows only and are not comparable
                with the published ones, see
                [`with_recomputed_adjusted_p_value`][gentropy.dataset.pathway_enrichment.PathwayEnrichment.with_recomputed_adjusted_p_value].
                Rows with a non-finite normalised enrichment score get a null adjusted p-value
                either way.
        """
        target_index = TargetIndex.from_parquet(session, target_index_path)

        pathway_index = (
            PathwayIndex.from_gmt(session, pathway_library_path)
            .resolve_pathway_ids(target_index, exclude_unmapped_pathways)
            .resolve_gene_ids(target_index)
        )
        pathway_index.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(pathway_index_path)

        pathway_enrichment = PathwayEnrichment.from_gsea_catalogue(
            session, pathway_enrichment_source_path
        ).with_degenerate_enrichment_masked()
        if recompute_missing_adjusted_p_value:
            pathway_enrichment = pathway_enrichment.with_recomputed_adjusted_p_value()
        pathway_enrichment.df.write.mode(session.write_mode).parquet(
            pathway_enrichment_path
        )
