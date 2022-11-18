"""Experiment to extract LD reference using hail."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import hydra
from pyspark.sql import functions as f

from etl.common.ETLSession import ETLSession
from etl.gwas_ingest.ld import variants_in_ld_in_gnomad_pop

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Experiment to extract LD reference for PICS.

    Args:
        cfg (DictConfig): configuration

    Returns:
        None
    """
    etl = ETLSession(cfg)
    hl.init(sc=etl.spark.sparkContext, default_reference="GRCh38")

    # Similar level of parallelism can help to tackle the problem:
    # associations = (
    #     etl.spark.read.parquet(ASSOCIATIONS)
    #     .repartition(400, "chr_id")
    #     .sortWithinPartitions("chr_pos")
    #     .alias("assoc")
    # ).persist()

    d = [
        {
            "variantId": "1_154453788_C_T",
            "pop": "nfe",
        },
        {"variantId": "21_13847526_T_C", "pop": "nfe"},
    ]
    variants_df = (
        etl.spark.createDataFrame(d)
        .toDF("pop", "variantId")
        .select(
            "*",
            f.split(f.col("variantId"), "_").getItem(0).alias("chrom"),
            f.split(f.col("variantId"), "_").getItem(1).cast("int").alias("pos"),
            f.split(f.col("variantId"), "_").getItem(2).alias("ref"),
            f.split(f.col("variantId"), "_").getItem(3).alias("alt"),
        )
        # .repartitionByRange("pop", "chrom")
        # .sortWithinPartitions("pos")
        # .persist()
    )

    # All populations represented in the association file
    populations = (
        variants_df.select("pop").distinct().rdd.flatMap(lambda x: x).collect()
    )

    # Retrieve LD information from gnomAD
    dataframes = []
    for pop in populations:
        pop_parsed_ldindex_path = ""
        pop_matrix_path = ""
        for popobj in cfg.etl.gwas_ingest.inputs.gnomad_populations:
            if popobj.id == pop:
                pop_parsed_ldindex_path = popobj.parsed_index
                pop_matrix_path = popobj.matrix
                variants_in_pop = variants_df.filter(f.col("pop") == pop).distinct()
                dataframes.append(
                    variants_in_ld_in_gnomad_pop(
                        etl=etl,
                        variants_df=variants_in_pop,
                        ld_path=pop_matrix_path,
                        parsed_ld_index_path=pop_parsed_ldindex_path,
                        min_r2=cfg.etl.gwas_ingest.parameters.min_r2,
                    )
                )
    etl.logger.info("Writing output...")
    for pop in populations:
        dataframes[pop.pop].write.parquet(f"gs://ot-team/dochoa/ld_tests/{pop}.parquet")

    return None


if __name__ == "__main__":
    main()
