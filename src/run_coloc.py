"""Compute all vs all Bayesian colocalisation analysis for all Genetics Portal.

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""


from __future__ import annotations

from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f
from etl.json import validate_df_schema

from otg.coloc.coloc import run_colocalisation
from otg.coloc.utils import _extract_credible_sets
from otg.common.session import ETLSession

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run colocalisation analysis."""
    etl = ETLSession(cfg)

    etl.logger.info("Colocalisation step started.")

    # Load data
    credible_sets = _extract_credible_sets(
        etl.spark.read.parquet(cfg.etl.coloc.inputs.study_locus_idx)
    )  # .filter(f.col("chromosome") == "22")  # for testing
    study_df = etl.spark.read.parquet(cfg.etl.coloc.inputs.study_idx).select(
        f.col("id").alias("studyId"),
        f.explode("traitFromSourceMappedIds").alias("phenotype"),
        "biofeature",
        "type",
        f.col("geneFromPhenotypeId").alias("gene_id"),
    )
    sumstats = etl.spark.read.parquet(cfg.etl.coloc.inputs.sumstats_filtered)

    coloc = run_colocalisation(
        credible_sets,
        study_df,
        cfg.etl.coloc.parameters.priorc1,
        cfg.etl.coloc.parameters.priorc2,
        cfg.etl.coloc.parameters.priorc12,
        cfg.etl.coloc.parameters.pp_threshold,
        sumstats,
    )
    validate_df_schema(coloc, "coloc.json")
    (
        coloc.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.coloc.outputs.coloc
        )
    )
    etl.logger.info("Colocalisation step finished.")


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
