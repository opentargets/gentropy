"""Step to run clump associations from summary statistics or study locus."""
from __future__ import annotations

from typing import Optional

from oxygen.common.session import Session
from oxygen.dataset.ld_index import LDIndex
from oxygen.dataset.study_index import StudyIndex
from oxygen.dataset.study_locus import StudyLocus
from oxygen.dataset.summary_statistics import SummaryStatistics


class ClumpStep:
    """Perform clumping of an association dataset to identify independent signals.

    Two types of clumping are supported and are applied based on the input dataset:
    - Clumping of summary statistics based on a window-based approach.
    - Clumping of study locus based on LD.

    Both approaches yield a StudyLocus dataset.
    """

    def __init__(
        self,
        session: Session,
        input_path: str,
        clumped_study_locus_path: str,
        study_index_path: Optional[str] = None,
        ld_index_path: Optional[str] = None,
        locus_collect_distance: Optional[int] = None,
    ) -> None:
        """Run the clumping step.

        Args:
            session (Session): Session object.
            input_path (str): Input path for the study locus or summary statistics files.
            clumped_study_locus_path (str): Output path for the clumped study locus dataset.
            study_index_path (Optional[str]): Input path for the study index dataset.
            ld_index_path (Optional[str]): Input path for the LD index dataset.
            locus_collect_distance (Optional[int]): Distance in base pairs to collect variants around the study locus.

        Raises:
            ValueError: If study index and LD index paths are not provided for study locus.
        """
        input_cols = session.spark.read.parquet(
            input_path, recursiveFileLookup=True
        ).columns
        if "studyLocusId" in input_cols:
            if study_index_path is None or ld_index_path is None:
                raise ValueError(
                    "Study index and LD index paths are required for clumping study locus."
                )
            study_locus = StudyLocus.from_parquet(session, input_path)
            ld_index = LDIndex.from_parquet(session, ld_index_path)
            study_index = StudyIndex.from_parquet(session, study_index_path)

            clumped_study_locus = study_locus.annotate_ld(
                study_index=study_index, ld_index=ld_index
            ).clump()
        else:
            sumstats = SummaryStatistics.from_parquet(
                session, input_path, recursiveFileLookup=True
            ).coalesce(4000)
            clumped_study_locus = sumstats.window_based_clumping(
                locus_collect_distance=locus_collect_distance
            )

        clumped_study_locus.df.write.mode(session.write_mode).parquet(
            clumped_study_locus_path
        )
