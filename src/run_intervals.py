from __future__ import annotations

import logging
from datetime import date
from functools import reduce
from typing import TYPE_CHECKING

import hydra
from pyspark.sql import SparkSession

from intervals.andersson2014 import ParseAndersson
from intervals.javierre2016 import ParseJavierre
from intervals.jung2019 import ParseJung
from intervals.Liftover import LiftOverSpark
from intervals.thurman2012 import ParseThurman

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(config_name="config")
def main(cfg: DictConfig) -> None:

    # establish spark connection
    spark = SparkSession.builder.master("yarn").getOrCreate()

    chain_file = cfg.intervals.liftover_chain_file
    max_difference = cfg.intervals.max_length_difference

    # Open and process gene file:
    gene_index = spark.read.parquet(cfg.intervals.gene_index).persist()

    # Initialize liftover object:
    lift = LiftOverSpark(chain_file, max_difference)

    # Parsing datasets:
    datasets = [
        # Parsing Andersson data:
        ParseAndersson(cfg.intervals.anderson_file, gene_index, lift).get_intervals(),
        # Parsing Javierre data:
        ParseJavierre(cfg.intervals.javierre_dataset, gene_index, lift).get_intervals(),
        # Parsing Jung data:
        ParseJung(cfg.intervals.jung_file, gene_index, lift).get_intervals(),
        # Parsing Thurman data:
        ParseThurman(cfg.intervals.thurman_file, gene_index, lift).get_intervals(),
    ]

    # Combining all datasets into a single dataframe, where missing columns are filled with nulls:
    df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), datasets)

    logging.info(f"Number of interval data: {df.count()}")
    logging.info(f"Writing data to: {cfg.intervals.output}")

    # Saving data:
    version = date.today().strftime("%y%m%d")
    (
        df.orderBy("chrom", "start")
        .repartition(200)
        .write.mode("overwrite")
        .parquet(cfg.intervals.output + f"/interval_{version}")
    )


if __name__ == "__main__":

    # Calling parsers:
    main()
