"""Compute all vs all Bayesian colocalisation analysis for all Genetics Portal.

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""


from __future__ import annotations

from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f

from etl.coloc_utils.coloc import run_colocalisation
from etl.common.ETLSession import ETLSession

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run colocalisation analysis."""
    etl = ETLSession(cfg)

    etl.logger.info("Colocalisation step started.")

    # Load data
    credible_sets = (
        etl.spark.read.parquet(cfg.etl.coloc.inputs.credible_set)
        .filter(f.col("chrom") == "22")  # for debugging
        .withColumn(
            "studyKey",
            f.xxhash64(*["type", "study_id", "phenotype_id", "bio_feature"]),
        )
        # Exclude studies without logABFs available
        .filter(f.col("logABF").isNotNull())
    )
    phenotype_id_gene = (
        etl.spark.read.option("header", "true")
        .option("sep", "\t")
        .csv(cfg.etl.coloc.inputs.phenotype_id_gene)
        .select(
            f.col("phenotype_id").alias("right_phenotype"),
            f.col("gene_id").alias("right_gene_id"),
        )
    )
    sumstats = etl.spark.read.parquet(cfg.etl.coloc.inputs.sumstats_filtered)

    coloc = run_colocalisation(
        etl,
        credible_sets,
        cfg.etl.coloc.parameters.priorc1,
        cfg.etl.coloc.parameters.priorc2,
        cfg.etl.coloc.parameters.priorc12,
        phenotype_id_gene,
        sumstats,
    )
    (
        coloc.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.coloc.outputs.coloc
        )
    )
    etl.logger.info("Colocalisation step finished.")


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
