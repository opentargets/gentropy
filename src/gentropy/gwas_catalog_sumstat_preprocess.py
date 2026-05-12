"""Step to generate variant annotation dataset."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field

from gentropy.common.session import Session
from gentropy.datasource.gwas_catalog.summary_statistics import (
    GWASCatalogSummaryStatistics,
)


class GWASCatalogSumstatsPreprocessDefaults(BaseModel, frozen=True):
    """Defaults for GWASCatalogSumstatsPreprocessStep.

    All values are frozen - create a new instance to override.
    """

    raw_sumstats_path: Annotated[
        str, Field(description="Input GWAS Catalog harmonised summary stats path.")
    ]
    out_sumstats_path: Annotated[
        str, Field(description="Output SummaryStatistics dataset path.")
    ]


class GWASCatalogSumstatsPreprocessStep:
    """Step to preprocess GWAS Catalog harmonised summary stats.

    It additionally performs sanity filter of GWAS before saving it.
    """

    def __init__(
        self,
        session: Session,
        config: GWASCatalogSumstatsPreprocessDefaults,
    ) -> None:
        """Run step to preprocess GWAS Catalog harmonised summary stats and produce SummaryStatistics dataset.

        Args:
            session (Session): Session object.
            config: Configuration for the step.
        """
        # Processing dataset:
        GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
            session.spark, config.raw_sumstats_path
        ).sanity_filter().df.write.mode(session.write_mode).parquet(
            config.out_sumstats_path
        )
        session.logger.info("Processing dataset successfully completed.")
