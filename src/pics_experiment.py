"""Experiment to extract LD reference using hail."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import hydra

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.pics import pics_study_locus

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Experiment to extract LD reference for PICS."""
    etl = ETLSession(cfg)
    hl.init(sc=etl.spark.sparkContext, default_reference="GRCh38")

    associations_df = etl.spark.read.parquet(
        "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_associations"
    )

    study_df = etl.spark.read.parquet(
        "gs://genetics_etl_python_playground/XX.XX/output/python_etl/parquet/gwas_catalog_studies/"
    )

    # A function that takes a study and a locus and returns the variants in LD with the index variant
    # in the study.
    study_locus = pics_study_locus(
        etl,
        associations_df,
        study_df,
        cfg.etl.gwas_ingest.inputs.gnomad_populations,
        cfg.etl.gwas_ingest.parameters.min_r2,
    )

    study_locus.write.mode("overwrite").parquet(
        "gs://ot-team/dochoa/pics/credset.parquet"
    )


if __name__ == "__main__":
    main()
