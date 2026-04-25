"""Step to run window based clumping on summary statistics datasts."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics


class WindowBasedClumpingDefaults(BaseModel, frozen=True):
    """Defaults for WindowBasedClumpingStep.

    All values are frozen - create a new instance to override.
    """

    gwas_significance: Annotated[
        float, Field(description="GWAS significance threshold.", default=1e-8)
    ]
    distance: Annotated[
        int,
        Field(
            description="Distance, within which tagging variants are collected around the semi-index.",
            default=500_000,
        ),
    ]
    collect_locus: Annotated[
        bool,
        Field(
            description="Whether to collect locus around semi-indices.", default=False
        ),
    ]
    collect_locus_distance: Annotated[
        int,
        Field(
            description="Distance, within which tagging variants are collected around the semi-index for locus collection.",
            default=500_000,
        ),
    ]
    inclusion_list_path: Annotated[
        str | None,
        Field(
            description="Path to the inclusion list (list of white-listed study identifier).",
            default=None,
        ),
    ] = None
    recursive_file_lookup: Annotated[
        bool,
        Field(
            description="Whether to recursively look for summary statistics files in the input path.",
            default=True,
        ),
    ]


class WindowBasedClumpingStep:
    """Apply window based clumping on summary statistics datasets."""

    def __init__(
        self,
        session: Session,
        config: WindowBasedClumpingDefaults,
        summary_statistics_input_path: str,
        study_locus_output_path: str,
    ) -> None:
        """Run window-based clumping step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
            summary_statistics_input_path (str): Path to the harmonized summary statistics dataset.
            study_locus_output_path (str): Output path for the resulting study locus dataset.
        """
        # If inclusion list path is provided, only these studies will be read:
        if config.inclusion_list_path:
            study_ids_to_ingest = [
                f"{summary_statistics_input_path}/{row['studyId']}.parquet"
                for row in session.spark.read.parquet(
                    config.inclusion_list_path
                ).collect()
            ]
        else:
            # If no inclusion list is provided, read all summary stats in folder:
            study_ids_to_ingest = [summary_statistics_input_path]

        ss = SummaryStatistics.from_parquet(
            session,
            study_ids_to_ingest,
            recursiveFileLookup=config.recursive_file_lookup,
        )

        # Clumping:
        study_locus = ss.window_based_clumping(
            distance=config.distance, gwas_significance=config.gwas_significance
        )

        # Optional locus collection:
        if config.collect_locus:
            # Collecting locus around semi-indices:
            study_locus = study_locus.annotate_locus_statistics(
                ss, collect_locus_distance=config.collect_locus_distance
            )

        study_locus.df.write.mode(session.write_mode).parquet(study_locus_output_path)
