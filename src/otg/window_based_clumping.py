"""Step to run window based clumping on summary statistics datasts."""
from __future__ import annotations

from dataclasses import dataclass, field

from omegaconf import MISSING

from otg.common.session import Session
from otg.dataset.summary_statistics import SummaryStatistics


@dataclass
class WindowBasedClumpingStep:
    """Apply window based clumping on summary statistics datasets.

    Attributes:
        session (Session): Session object.
        summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
        study_locus_output_path (str): Output path for the resulting study locus dataset.
        inclusion_list_path (str | None): Path to the inclusion list (list of white-listed study identifier). Optional.
        locus_collect_distance (int | None): Distance, within which tagging variants are collected around the semi-index. Optional.
    """

    session: Session = MISSING
    summary_statistics_input_path: str = MISSING
    study_locus_output_path: str = MISSING
    inclusion_list_path: str | None = field(default=None)
    locus_collect_distance: int | None = field(default=None)

    def __post_init__(self: WindowBasedClumpingStep) -> None:
        """Run the clumping step."""
        # If inclusion list path is provided, only these studies will be read:
        if self.inclusion_list_path:
            study_ids_to_ingest = [
                f'{self.summary_statistics_input_path}/{row["studyId"]}.parquet'
                for row in self.session.spark.read.parquet(
                    self.inclusion_list_path
                ).collect()
            ]
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [self.summary_statistics_input_path]

        (
            SummaryStatistics.from_parquet(
                self.session,
                study_ids_to_ingest,
                recursiveFileLookup=True,
            )
            .coalesce(4000)
            # Applying window based clumping:
            .window_based_clumping(locus_collect_distance=self.locus_collect_distance)
            # Save resulting study locus dataset:
            .df.write.mode(self.session.write_mode)
            .parquet(self.study_locus_output_path)
        )
