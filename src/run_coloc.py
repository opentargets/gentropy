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
    colocWithGenes = addMolecularTraitPhenotypeGenes(
        spark, coloc, cfg.coloc.phenotype_id_gene
    )

    # # 4. Add more info from sumstats (metadata)
    # colocWithAllMetadata = addColocSumstatsInfo(
    #     spark, colocWithGenes, cfg.coloc.sumstats_filtered
    # )

    # Write output
    (
        colocWithGenes.write
        # .partitionBy("left_chrom")
        .mode("overwrite").parquet(cfg.coloc.output)
    )

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


# Expected result
# # -RECORD 0-------------------------------------
# #  left_key              | NEALE2_20003_1140...
# #  left_lead_variant_id  | 22:16590692:CAA:C
# #  left_type             | gwas
# #  left_key              | NEALE2_20003_1140...
# #  right_key             | GTEx-sQTL_chr22:1...
# #  right_lead_variant_id | 22:17806438:G:A
# #  right_type            | sqtl
# #  right_key             | GTEx-sQTL_chr22:1...
# #  coloc_n_vars          | 64
# #  left_logsum           | 7.661225695815798
# #  right_logsum          | 9.419701640007158
# #  logsum_left_right     | 10.026044478423552
# #  priorc1               | 1.0E-4
# #  priorc2               | 1.0E-4
# #  priorc12              | 1.0E-5
# #  lH0abf                | 0
# #  lH1abf                | -1.549114676160384
# #  lH2abf                | 0.20936126803097643
# #  lH3abf                | -1.3406169647338686
# #  lH4abf                | -1.4868809865466766
# #  coloc_h0              | 0.3409377045755464
# #  coloc_h1              | 0.07242743035215946
# #  coloc_h2              | 0.4203387963569506
# #  coloc_h3              | 0.0892179998555815
# #  coloc_h4              | 0.07707806885976214
# #  coloc_h4_h3           | 0.8639295768177896
# #  coloc_log2_h4_h3      | -0.21101437892981328

if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
