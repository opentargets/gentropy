"""Step to run GWASCatalog ingestion."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.pics import pics_all_study_locus


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run GWASCatalog ingestion."""
    etl = ETLSession(cfg)
    hl.init(sc=etl.spark.sparkContext, default_reference="GRCh38")

    # etl.logger.info("Ingesting GWAS Catalog association data...")
    #
    # # Ingesting GWAS Catalog associations:
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

    # # # Read saved association data:
    # # assoc = etl.spark.read.parquet(
    # #     "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_associations_patched"
    # # )

    # # Ingesting GWAS Catalog studies:
    # gwas_studies = ingest_gwas_catalog_studies(
    #     etl,
    #     cfg.etl.gwas_ingest.inputs.gwas_catalog_studies,
    #     cfg.etl.gwas_ingest.inputs.gwas_catalog_ancestries,
    #     cfg.etl.gwas_ingest.inputs.summary_stats_list,
    # )
    # validate_df_schema(gwas_studies, "studies.json")
    # etl.logger.info(
    #     f"Writing studies data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_studies}"
    # )
    # gwas_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
    #     cfg.etl.gwas_ingest.outputs.gwas_catalog_studies
    # )

    # # Joining study and association
    # study_assoc = assoc.join(gwas_studies, on="studyAccession", how="outer").transform(
    #     spliting_gwas_studies
    # )

    # # Extracting study table and save:
    # (
    #     study_assoc.transform(generate_study_table)
    #     .write.mode(cfg.environment.sparkWriteMode)
    #     .parquet(cfg.etl.gwas_ingest.outputs.gwas_catalog_studies)
    # )

    # # Extracting associations for PICS and save:
    # (
    #     study_assoc.transform(prepare_associations_for_pics)
    #     .write.mode(cfg.environment.sparkWriteMode)
    #     .parquet(
    #         "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_PICS_ready"
    #     )
    # )

    pics_ready = etl.spark.read.parquet(
        "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_PICS_ready"
    )

    # A function that takes a study and a locus and returns the variants in LD with the index variant
    # in the study.
    study_locus = pics_all_study_locus(
        etl,
        pics_ready,
        cfg.etl.gwas_ingest.inputs.gnomad_populations,
        cfg.etl.gwas_ingest.parameters.min_r2,
        cfg.etl.gwas_ingest.parameters.k,
    )

    study_locus.write.mode("overwrite").parquet(
        "gs://ot-team/dochoa/pics/credset.parquet"
    )


if __name__ == "__main__":

    main()
