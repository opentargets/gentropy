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
    every step downstream reads a validated dataset: the gene symbols of the library are
    resolved into the Ensembl gene identifiers of a given target index release, and the
    identifier of each pathway in its source ontology is kept.
    """

    def __init__(
        self,
        session: Session,
        pathway_library_path: str,
        pathway_enrichment_source_path: str,
        target_index_path: str,
        pathway_index_path: str,
        pathway_enrichment_path: str,
    ) -> None:
        """Run the pathway ingestion step.

        Args:
            session (Session): Session object.
            pathway_library_path (str): Path to the pathway library, as a GMT file.
            pathway_enrichment_source_path (str): Path to the enrichment results of the same
                library, partitioned by `diseaseId`.
            target_index_path (str): Path to the target index the gene symbols are resolved
                against.
            pathway_index_path (str): Output path of the harmonised `PathwayIndex` dataset.
            pathway_enrichment_path (str): Output path of the harmonised `PathwayEnrichment`
                dataset.
        """
        target_index = TargetIndex.from_parquet(session, target_index_path)

        pathway_index = PathwayIndex.from_gmt(
            session, pathway_library_path
        ).resolve_gene_ids(target_index)
        pathway_index.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(pathway_index_path)

        pathway_enrichment = PathwayEnrichment.from_gsea_catalogue(
            session, pathway_enrichment_source_path
        )
        pathway_enrichment.df.write.mode(session.write_mode).parquet(
            pathway_enrichment_path
        )
