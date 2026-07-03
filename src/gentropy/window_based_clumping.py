"""Step to run window based clumping on summary statistics datasts."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics


class WindowBasedClumpingStep:
    """Identify lead variants from summary statistics using window-based clumping.

    Algorithm
    ---------
    1. **Filter** — retain only variants with p-value ≤ `gwas_significance`.
    2. **Cluster** — chain neighbouring significant variants into proximity clusters:
       a contiguous group where every consecutive pair (sorted by position) is within
       `distance` bp on the same chromosome and study. Given 2 subsequent variants
       A and B are more distant to each other then `distance` parameter, they
       will be assigned to different clusters.
    3. **Rank** — within each cluster, rank variants by significance
       (ascending pValueExponent, then ascending pValueMantissa).
    4. **Prune** — iterate variants in significance order; pick a new lead only if no
       already-chosen lead lies within `distance` to current lead. Non-leads are flagged
       `WINDOW_CLUMPED` and removed, leaving one lead per non-overlapping window.
    5. **Bound** — assign `locusStart = max(0, position − distance)` and
       `locusEnd = position + distance` to each lead.
    6. **Annotate locus** — two modes controlled by `collect_locus`:

       - ``collect_locus=True``: collect all sumstat variants within
         `collect_locus_distance` of each lead into a `locus[]` array
         (full neighbourhood for downstream tools that need tag-variant stats).
       - ``collect_locus=False`` *(default)*: annotate only the sentinel variant
         itself as a single-element `locus[]`.

    7. **Sort** — order the output by studyId, chromosome, position.
    8. **Write** — partition by studyId and write to Parquet. The output is a StudyLocus dataset.
    """

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_locus_output_path: str,
        distance: int,
        gwas_significance: float,
        collect_locus: bool,
        collect_locus_distance: int,
        inclusion_list_path: str | None,
        recursive_file_lookup: bool,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
            distance (int): Clumping window half-width in base pairs. Variants within this
                distance of a chosen lead are suppressed. Also defines locusStart/locusEnd bounds.
            gwas_significance (float): P-value threshold for pre-filtering summary statistics
                before clumping (e.g. 5e-8).
            collect_locus (bool): If True, collect all sumstat variants within
                `collect_locus_distance` of each lead into locus[]. If False, annotate only
                the sentinel variant as a single-element locus[].
            collect_locus_distance (int): Half-width in base pairs for locus collection.
                Only used when `collect_locus=True`.
            inclusion_list_path (str | None): Path to a Parquet file with a `studyId` column.
                When provided, only the listed studies are read from the input path.
            recursive_file_lookup (bool): Whether to search the input path recursively for
                summary statistics files.
        """
        # If inclusion list path is provided, only these studies will be read:
        if inclusion_list_path:
            study_ids_to_ingest = [
                f"{summary_statistics_input_path}/{row['studyId']}.parquet"
                for row in session.spark.read.parquet(inclusion_list_path).collect()
            ]
            # Force recursive file lookup if inclusion list is provided
            recursive_file_lookup = True
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [summary_statistics_input_path]

        ss = SummaryStatistics.from_parquet(
            session, study_ids_to_ingest, recursiveFileLookup=recursive_file_lookup
        )

        # Clumping:
        study_locus = ss.window_based_clumping(
            distance=distance, gwas_significance=gwas_significance
        )

        # Optional locus collection:
        if collect_locus:
            # Collecting locus around semi-indices:
            study_locus = study_locus.annotate_locus_statistics_by_distance(
                ss, collect_locus_distance=collect_locus_distance
            )
        else:
            # or just annotating study locus with sentinel variant information
            study_locus = study_locus.annotate_locus_by_sentinel_variant(ss)
        (
            study_locus.df.orderBy("studyLocusId", "chromosome", "position")
            .write.mode(session.write_mode)
            .partitionBy("studyLocusId")
            .parquet(study_locus_output_path)
        )
