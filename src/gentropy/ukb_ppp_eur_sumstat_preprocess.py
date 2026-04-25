"""Step to run UKB PPP (EUR) data ingestion."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.processing import prepare_va
from gentropy.common.session import Session
from gentropy.datasource.ukb_ppp_eur.study_index import UkbPppEurStudyIndex
from gentropy.datasource.ukb_ppp_eur.summary_stats import UkbPppEurSummaryStats


class UkbPppEurStepConfig(BaseModel, frozen=True):
    """Defaults for UkbPppEurStep.

    All values are frozen - create a new instance to override.
    """

    raw_study_index_path_from_tsv: Annotated[
        str, Field(description="Input raw study index path.")
    ]
    raw_summary_stats_path: Annotated[
        str, Field(description="Input raw summary stats path.")
    ]
    variant_annotation_path: Annotated[
        str, Field(description="Input variant annotation dataset path.")
    ]
    tmp_variant_annotation_path: Annotated[
        str, Field(description="Temporary output path for variant annotation dataset.")
    ]
    study_index_output_path: Annotated[
        str, Field(description="Study index output path.")
    ]
    summary_stats_output_path: Annotated[
        str, Field(description="Summary stats output path.")
    ]


class UkbPppEurStep:
    """UKB PPP (EUR) data ingestion and harmonisation."""

    def __init__(self, config: UkbPppEurStepConfig, session: Session) -> None:
        """Run UKB PPP (EUR) data ingestion and harmonisation step.

        Args:
            config (UkbPppEurStepConfig): Configuration for the step.
            session (Session): Session object.
        """
        session.logger.info(
            "Pre-compute the direct and flipped variant annotation dataset."
        )
        prepare_va(
            session, config.variant_annotation_path, config.tmp_variant_annotation_path
        )

        session.logger.info("Process study index.")
        (
            UkbPppEurStudyIndex.from_source(
                spark=session.spark,
                raw_study_index_path_from_tsv=config.raw_study_index_path_from_tsv,
                raw_summary_stats_path=config.raw_summary_stats_path,
            )
            .df.write.mode("overwrite")
            .parquet(config.study_index_output_path)
        )

        session.logger.info("Process and harmonise summary stats.")
        UkbPppEurSummaryStats.process_summary_stats_per_chromosome(
            session,
            config.raw_summary_stats_path,
            config.tmp_variant_annotation_path,
            config.summary_stats_output_path,
            config.study_index_output_path,
        )
