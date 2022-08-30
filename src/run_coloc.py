"""
Compute all vs all Bayesian colocalisation analysis for all Genetics Portal

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""


from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

from coloc_utils.coloc import colocalisation
from coloc_utils.coloc_metadata import add_moleculartrait_phenotype_genes
from coloc_utils.overlaps import find_all_vs_all_overlapping_signals
from common.ETLSession import ETLSession

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """
    Run colocalisation analysis
    """

    # establish spark connection
    etl = ETLSession(cfg)

    etl.logger.info("Colocalisation step started")

    # 1. Looking for overlapping signals
    overlapping_signals = find_all_vs_all_overlapping_signals(
        etl.spark, cfg.etl.coloc.inputs.credible_set
    )

    # 2. Perform colocalisation analysis
    coloc = colocalisation(
        overlapping_signals,
        cfg.etl.coloc.parameters.priorc1,
        cfg.etl.coloc.parameters.priorc2,
        cfg.etl.coloc.parameters.priorc12,
    )

    # 3. Add molecular trait genes (metadata)
    coloc_with_genes = add_moleculartrait_phenotype_genes(
        etl.spark, coloc, cfg.etl.coloc.inputs.phenotype_id_gene
    )

    # 4. Add betas from sumstats
    # Adds backwards compatibility with production schema
    # Note: First implementation in add_coloc_sumstats_info hasn't been fully tested
    # colocWithAllMetadata = addColocSumstatsInfo(
    #     spark, coloc_with_genes, cfg.coloc.sumstats_filtered
    # )

    # Writing colocalisation results
    (
        coloc_with_genes.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.coloc.outputs.coloc
        )
    )

    etl.logger.info(f"Number of colocalisations: {coloc_with_genes.count()}")
    etl.logger.info("Colocalisation step finished")


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
