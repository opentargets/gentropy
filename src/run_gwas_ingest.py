from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.process_associations import ingest_gwas_catalog_associations


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:

    # establish spark connection
    etl = ETLSession(cfg)

    etl.logger.info("Ingesting GWAS Catalog data...")

    assoc = ingest_gwas_catalog_associations(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_associations,
        cfg.etl.gwas_ingest.inputs.variant_annotation,
        cfg.etl.gwas_ingest.parameters.p_value_cutoff,
    )

    etl.logger.info(
        f"Writing data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_associations}"
    )

    (
        assoc.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
        )
    )


if __name__ == "__main__":

    main()
