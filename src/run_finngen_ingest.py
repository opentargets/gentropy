"""Steps to run FinnGen study table ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

from etl.common.ETLSession import ETLSession
from etl.finngen_ingest.finngen_ingestion import ingest_finngen_studies

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run FinnGen study table ingestion."""
    etl = ETLSession(cfg)

    etl.logger.info("Ingesting FinnGen study data...")
    finngen_studies = ingest_finngen_studies(
        etl=etl, phenotype_table_url=cfg.etl.finngen_ingest.inputs.phenotype_table_url
    )

    etl.logger.info("Saving FinnGen study data...")
    finngen_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.finngen_ingest.outputs.finngen_catalog_studies
    )


if __name__ == "__main__":
    main()
