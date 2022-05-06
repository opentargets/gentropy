"""
Compute all vs all Bayesian colocalisation analysis for all Genetics Portal

This script calculates posterior probabilities of different causal variants
configurations under the assumption of a single causal variant for each trait.

Logic reproduced from: https://github.com/chr1swallace/coloc/blob/main/R/claudia.R
"""

import os
import hydra
from pyspark import SparkConf
from pyspark.sql import SparkSession
from omegaconf import DictConfig
from colocMetadata import addColocSumstatsInfo, addMolecularTraitPhenotypeGenes
from coloc import colocalisation
from overlaps import findOverlappingSignals


@hydra.main(config_path=os.getcwd(), config_name="config")
def main(cfg: DictConfig) -> None:
    """
    Run colocalisation analysis
    """

    sparkConf = SparkConf()
    sparkConf = sparkConf.set("spark.hadoop.fs.gs.requester.pays.mode", "AUTO")
    sparkConf = sparkConf.set(
        "spark.hadoop.fs.gs.requester.pays.project.id", "open-targets-eu-dev"
    )

    # establish spark connection
    spark = SparkSession.builder.config(conf=sparkConf).master("local[*]").getOrCreate()

    # 1. Obtain overlapping signals in OT genetics portal
    overlappingSignals = findOverlappingSignals(spark, cfg.coloc.credible_set)

    # 2. Perform colocalisation analysis
    coloc = colocalisation(
        overlappingSignals,
        cfg.coloc.priorc1,
        cfg.coloc.priorc2,
        cfg.coloc.priorc12,
    )

    # 3. Add molecular trait genes (metadata)
    colocWithMetadata = addMolecularTraitPhenotypeGenes(
        spark, coloc, cfg.coloc.phenotype_id_gene
    )

    # 4. Add more info from sumstats (metadata)
    colocWithMetadata = addColocSumstatsInfo(spark, coloc, cfg.coloc.sumstats_filtered)

    # Write output
    (colocWithMetadata.write.mode("overwrite").parquet(cfg.coloc.output))

    # TODO: compute model averaged effect size ratios
    # https://github.com/tobyjohnson/gtx/blob/9afa9597a51d0ff44536bc5c8eddd901ab3e867c/R/coloc.R#L91

    # For debugging
    # (
    #     coloc
    #     .filter(
    #         (F.col("left_studyKey") == "gwas_NEALE2_20003_1140909872") &
    #         (F.col("right_studyKey") ==
    # "sqtl_GTEx-sQTL_chr22:17791301:17806239:clu_21824:ENSG00000243156_Ovary") &
    #         (F.col("left_lead_variant_id") == "22:16590692:CAA:C") &
    #         (F.col("right_lead_variant_id") == "22:17806438:G:A"))
    #     .show(vertical = True)
    # )


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
