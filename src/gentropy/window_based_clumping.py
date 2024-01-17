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
        inclusion_list_path: str | None = None,
        locus_collect_distance: int | None = None,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
            inclusion_list_path (str | None): Path to the inclusion list (list of white-listed study identifier). Optional.
            locus_collect_distance (int | None): Distance, within which tagging variants are collected around the semi-index. Optional.
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

        (
            SummaryStatistics.from_parquet(
                session,
                study_ids_to_ingest,
                recursiveFileLookup=True,
            )
            .coalesce(4000)
            # Applying window based clumping:
            .window_based_clumping(locus_collect_distance=locus_collect_distance)
            # Save resulting study locus dataset:
            .df.write.mode(session.write_mode)
            .parquet(study_locus_output_path)
        )
