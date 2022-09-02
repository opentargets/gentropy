from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from common.ETLSession import ETLSession
from gwas_ingest.process_associations import ingest_gwas_catalog_associations


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:

    # establish spark connection
    etl = ETLSession(cfg)

    # Extract output folder:
    output_folder = cfg.etl.gwas_ingest.outputs.gwas_output_folder

    etl.logger.info("Ingesting GWAS Catalog data...")

    assoc = ingest_gwas_catalog_associations(etl, cfg)

    (
        assoc.write.mode(cfg.environment.sparkWriteMode).parquet(
            f"{output_folder}/assoc_test"
        )
    )


if __name__ == "__main__":

    main()
