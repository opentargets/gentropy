"""Step to run FinnGen study index generation."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.finngen.efo_mapping import EFOMapping
from gentropy.datasource.finngen.study_index import FinnGenStudyIndex


class FinnGenStudiesDefaults(BaseModel, frozen=True):
    """Defaults for FinnGenStudiesStep.

    All values are frozen - create a new instance to override.
    """

    finngen_phenotype_table_url: Annotated[
        str,
        Field(description="URL to the FinnGen phenotype table."),
    ]
    finngen_release_prefix: Annotated[
        str,
        Field(default="FINNGEN_R11_", description="FinnGen release prefix."),
    ]
    finngen_summary_stats_url_prefix: Annotated[
        str,
        Field(
            default="gs://finngen-public-data-r11/summary_stats/finngen_R11_",
            description="FinnGen summary stats URL prefix.",
        ),
    ]
    finngen_summary_stats_url_suffix: Annotated[
        str,
        Field(default=".gz", description="FinnGen summary stats URL suffix."),
    ]
    efo_curation_mapping_url: Annotated[
        str,
        Field(description="URL to the EFO curation mapping file."),
    ]
    sample_size: Annotated[
        int,
        Field(
            default=453733,
            description="Number of individuals that participated in sample collection, derived from finngen release metadata.",
        ),
    ]


class FinnGenStudiesStep:
    """FinnGen study index generation step."""

    def __init__(
        self,
        session: Session,
        config: FinnGenStudiesDefaults,
        finngen_study_index_out: str,
    ) -> None:
        """Run FinnGen study index generation step.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
            finngen_study_index_out (str): Output FinnGen study index path.
        """
        _match = FinnGenStudyIndex.validate_release_prefix(
            config.finngen_release_prefix
        )
        release_prefix = _match["prefix"]
        release = _match["release"]

        efo_mapping = EFOMapping.from_path(session, config.efo_curation_mapping_url)
        study_index = FinnGenStudyIndex.from_source(
            session.spark,
            config.finngen_phenotype_table_url,
            release_prefix,
            config.finngen_summary_stats_url_prefix,
            config.finngen_summary_stats_url_suffix,
            config.sample_size,
        )
        study_index_with_efo = efo_mapping.annotate_study_index(study_index, release)
        study_index_with_efo.df.coalesce(session.output_partitions).write.mode(
            session.write_mode
        ).parquet(finngen_study_index_out)
