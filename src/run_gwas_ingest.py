"""Step to run GWASCatalog ingestion."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.study_ingestion import ingest_gwas_catalog_studies


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run GWASCatalog ingestion."""
    etl = ETLSession(cfg)

    etl.logger.info("Ingesting GWAS Catalog association data...")

    # # This section is commented out for testing study ingestion:
    # assoc = ingest_gwas_catalog_associations(
    #     etl,
    #     cfg.etl.gwas_ingest.inputs.gwas_catalog_associations,
    #     cfg.etl.variant_annotation.outputs.variant_annotation,
    #     cfg.etl.gwas_ingest.parameters.p_value_cutoff,
    # )

    # etl.logger.info(
    #     f"Writing associations data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_associations}"
    # )
    # (
    #     assoc.write.mode(cfg.environment.sparkWriteMode).parquet(
    #         cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
    #     )
    # )
    # etl.logger.info("Ingesting GWAS Catalog Studies...")

    # Loading saved association file:
    assoc = etl.spark.read.parquet(
        cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
    )

    # Ingest GWAS Catalog studies:
    gwas_studies = ingest_gwas_catalog_studies(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_studies,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_ancestries,
        cfg.etl.gwas_ingest.inputs.summary_stats_list,
    )
    etl.logger.info(
        f"Writing studies data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_studies}"
    )

    # Saving temporary output:
    gwas_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.gwas_ingest.outputs.gwas_catalog_studies
    )

    # Joining study with associations:
    (gwas_studies.join(assoc, on="studyAccession", how="outer").persist())


if __name__ == "__main__":

    main()
