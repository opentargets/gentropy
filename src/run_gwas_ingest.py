"""Step to run GWASCatalog ingestion."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.process_associations import (
    ingest_gwas_catalog_associations,
    prepare_associations_for_pics,
)
from etl.gwas_ingest.study_ingestion import (
    generate_study_table,
    ingest_gwas_catalog_studies,
    spliting_gwas_studies,
)


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run GWASCatalog ingestion."""
    etl = ETLSession(cfg)

    etl.logger.info("Ingesting GWAS Catalog association data...")

    # Ingesting GWAS Catalog associations:
    assoc = ingest_gwas_catalog_associations(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_associations,
        cfg.etl.variant_annotation.outputs.variant_annotation,
        cfg.etl.gwas_ingest.parameters.p_value_cutoff,
    )

    etl.logger.info(
        f"Writing associations data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_associations}"
    )
    (
        assoc.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
        )
    )
    etl.logger.info("Ingesting GWAS Catalog Studies...")

    # # Read saved association data:
    # assoc = etl.spark.read.parquet(
    #     "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_associations_patched"
    # )

    # Ingesting GWAS Catalog studies:
    gwas_studies = ingest_gwas_catalog_studies(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_studies,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_ancestries,
        cfg.etl.gwas_ingest.inputs.summary_stats_list,
    )
    etl.logger.info(
        f"Writing studies data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_studies}"
    )
    gwas_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.gwas_ingest.outputs.gwas_catalog_studies
    )

    # Joining study and association
    study_assoc = assoc.join(gwas_studies, on="studyAccession", how="outer").transform(
        spliting_gwas_studies
    )

    # Extracting study table and save:
    (
        study_assoc.transform(generate_study_table)
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.gwas_ingest.outputs.gwas_catalog_studies)
    )

    # Extracting associations for PICS and save:
    (
        study_assoc.transform(prepare_associations_for_pics)
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(
            "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_PICS_ready"
        )
    )


if __name__ == "__main__":

    main()
