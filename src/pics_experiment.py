"""Experiment to extract LD reference using hail."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import hydra
from pyspark.sql import functions as f

from etl.common.ETLSession import ETLSession
from etl.v2d.ld import gnomad_toploci_ld_annotation

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
        {
            "variantId": "1_100236660_T_A",
            "pop": "afr",
        },
        {"variantId": "21_13847526_T_C", "pop": "nfe"},
    ]
    lead_df = (
        etl.spark.createDataFrame(d)
        .toDF("pop", "variantId")
        .select(
            "*",
            f.split(f.col("variantId"), "_").getItem(0).alias("chrom"),
            f.split(f.col("variantId"), "_").getItem(1).alias("pos"),
            f.split(f.col("variantId"), "_").getItem(2).alias("ref"),
            f.split(f.col("variantId"), "_").getItem(2).alias("alt"),
        )
        .repartitionByRange("pop", "chrom")
        .sortWithinPartitions("pos")
        .persist()
    )

    # All populations in lead variants
    populations = lead_df.select("pop").distinct().rdd.flatMap(lambda x: x).collect()

    dataframes = []
    # Looping through all populations:
    for pop in populations:
        variants = lead_df.filter(f.col("pop") == pop).distinct()
        dataframes.append(
            gnomad_toploci_ld_annotation(
                etl=etl,
                lead_df=variants,
                population=pop,
                version=cfg.etl.gwas_ingest.parameters.ld_gnomad_version,
                ld_radius=cfg.etl.gwas_ingest.parameters.ld_window,
                min_r2=cfg.etl.gwas_ingest.parameters.min_r2,
            )
        )

    etl.logger.info("Writing output...")
    for pop in populations:
        dataframes[pop.pop].write.parquet(f"gs://ot-team/dochoa/ld_tests/{pop}.parquet")

    return None


if __name__ == "__main__":
    main()
