"""Step to run GWASCatalog ingestion."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.pics import pics_all_study_locus
from etl.gwas_ingest.process_associations import ingest_gwas_catalog_associations
from etl.gwas_ingest.study_ingestion import (
    generate_study_table,
    ingest_gwas_catalog_studies,
    spliting_gwas_studies,
)


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run GWASCatalog ingestion."""
    etl = ETLSession(cfg)
    # hl.init(sc=etl.spark.sparkContext, default_reference="GRCh38")
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

    # Read saved association data:
    assoc = etl.spark.read.parquet(
        cfg.etl.gwas_ingest.outputs.gwas_catalog_associations
    )

    # Ingesting GWAS Catalog studies:
    gwas_studies = ingest_gwas_catalog_studies(
        etl,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_studies,
        cfg.etl.gwas_ingest.inputs.gwas_catalog_ancestries,
        cfg.etl.gwas_ingest.inputs.summary_stats_list,
    )

    (
        gwas_studies.write.mode("overwrite").parquet(
            "gs://ot-team/dsuveges/pre-split-gwas-studies"
        )
    )
    etl.logger.info(
        f"Writing studies data to: {cfg.etl.gwas_ingest.outputs.gwas_catalog_studies}"
    )
    gwas_studies.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.gwas_ingest.outputs.gwas_catalog_studies
    )

    # Joining study and association
    study_assoc = gwas_studies.join(assoc, on="studyAccession", how="left").transform(
        spliting_gwas_studies
    )

    # Extracting study table and save:
    (
        study_assoc.transform(generate_study_table)
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.gwas_ingest.outputs.gwas_catalog_studies)
    )
    assoc_columns = [
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
        "variantId",
        "studyId",
        "pValueMantissa",
        "pValueExponent",
        "beta",
        "beta_ci_lower",
        "beta_ci_upper",
        "odds_ratio",
        "odds_ratio_ci_lower",
        "odds_ratio_ci_upper",
        "qualityControl",
    ]
    studies = etl.spark.read.parquet(cfg.etl.gwas_ingest.outputs.gwas_catalog_studies)
    associations = study_assoc.select(*assoc_columns)

    s = studies  # .filter(f.col("studyId").startswith(study_id))
    a = associations  # .filter(f.col("studyId").startswith(study_id))
    print(f"Number of studies: {s.count()}")
    print(f"Number of associations: {a.count()}")
    # Running PICS:
    pics_data = pics_all_study_locus(
        etl,
        a,
        s,
        cfg.etl.gwas_ingest.inputs.gnomad_populations,
        cfg.etl.gwas_ingest.parameters.min_r2,
        cfg.etl.gwas_ingest.parameters.k,
    )
    pics_data.write.mode("overwrite").parquet(
        cfg.etl.gwas_ingest.outputs.pics_credible_set
    )


if __name__ == "__main__":

    main()
