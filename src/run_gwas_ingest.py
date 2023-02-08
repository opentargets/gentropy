"""Step to run GWASCatalog ingestion."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.pics import pics_all_study_locus
from etl.gwas_ingest.process_associations import (
    generate_association_table,
    ingest_gwas_catalog_associations,
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

    hl.init(sc=etl.spark.sparkContext, default_reference="GRCh38")
    etl.logger.info("Ingesting GWAS Catalog association data...")

    # Ingesting GWAS Catalog associations:
    raw_associations = ingest_gwas_catalog_associations(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_associations,
        cfg.etl.variant_annotation.outputs.variant_annotation,
        cfg.etl.gwas_ingest.parameters.p_value_cutoff,
    )

    # Joining study and association
    study_assoc = (
        # Ingesting GWAS Catalog studies:
        ingest_gwas_catalog_studies(
            etl,
            cfg.etl.gwas_ingest.inputs.gwas_catalog_studies,
            cfg.etl.gwas_ingest.inputs.gwas_catalog_ancestries,
            cfg.etl.gwas_ingest.inputs.summary_stats_list,
        )
        # Joining with associations:
        .join(raw_associations, on="studyAccession", how="left")
        # Splitting studies:
        .transform(spliting_gwas_studies)
    )

    # Extracting study table and save:
    studies = generate_study_table(study_assoc)
    etl.logger.info(
        f"Writing studies data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_studies}"
    )
    (
        studies.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.gwas_ingest.outputs.gwas_catalog_studies
        )
    )

    # Extracting associations from the combined study/assoc table:
    associations = generate_association_table(study_assoc)

    # Saved association data:
    etl.logger.info(
        f"Writing associations data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_associations}"
    )
    (
        associations.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
        )
    )

    # Running PICS:
    pics_data = pics_all_study_locus(
        etl,
        associations,
        studies,
        cfg.etl.gwas_ingest.inputs.gnomad_populations,
        cfg.etl.gwas_ingest.parameters.min_r2,
        cfg.etl.gwas_ingest.parameters.k,
    )
    pics_data.write.mode("overwrite").parquet(
        cfg.etl.gwas_ingest.outputs.pics_credible_set
    )


if __name__ == "__main__":
    main()
