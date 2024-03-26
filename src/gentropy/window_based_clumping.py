"""Step to run window based clumping on summary statistics datasts."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics


class WindowBasedClumpingStep:
    """Apply window based clumping on summary statistics datasets."""

    def __init__(
        self,
        session: Session,
        summary_statistics_input_path: str,
        study_locus_output_path: str,
        distance: int = 500_000,
        collect_locus: bool = False,
        collect_locus_distance: int = 500_000,
        inclusion_list_path: str | None = None,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
            distance (int): Distance, within which tagging variants are collected around the semi-index. Optional.
            collect_locus (bool): Whether to collect locus around semi-indices. Optional.
            collect_locus_distance (int): Distance, within which tagging variants are collected around the semi-index. Optional.
            inclusion_list_path (str | None): Path to the inclusion list (list of white-listed study identifier). Optional.
        """
        # If inclusion list path is provided, only these studies will be read:
        if inclusion_list_path:
            study_ids_to_ingest = [
                f'{summary_statistics_input_path}/{row["studyId"]}.parquet'
                for row in session.spark.read.parquet(inclusion_list_path).collect()
            ]
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [summary_statistics_input_path]

        ss = SummaryStatistics.from_parquet(
            session,
            study_ids_to_ingest,
            recursiveFileLookup=True,
        )

        # Clumping:
        study_locus = ss.window_based_clumping(
            distance=distance,
        )

        # Optional locus collection:
        if collect_locus:
            # Collecting locus around semi-indices:
            study_locus = study_locus.annotate_locus_statistics(
                ss, collect_locus_distance=collect_locus_distance
            )

        study_locus.df.write.mode(session.write_mode).parquet(study_locus_output_path)
