"""Steps to run FinnGen study table ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

from etl.common.ETLSession import ETLSession
from etl.finngen_ingest.finngen_ingestion import ingest_finngen_studies
from etl.json import validate_df_schema

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run FinnGen study table ingestion."""
    etl = ETLSession(cfg)

    etl.logger.info("Ingesting FinnGen study data...")
    finngen_studies = ingest_finngen_studies(
        etl=etl,
        phenotype_table_url=cfg.etl.finngen_ingest.inputs.phenotype_table_url,
        finngen_release_prefix=cfg.etl.finngen_ingest.parameters.finngen_release_prefix,
        sumstat_url_prefix=cfg.etl.finngen_ingest.parameters.sumstat_url_prefix,
        sumstat_url_suffix=cfg.etl.finngen_ingest.parameters.sumstat_url_suffix,
    )

    etl.logger.info("Saving FinnGen study data...")
    finngen_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.finngen_ingest.outputs.finngen_catalog_studies
    )

    etl.logger.info("Validating FinnGen study data against the schema...")
    validate_df_schema(finngen_studies, "studies.json")


if __name__ == "__main__":
    main()
