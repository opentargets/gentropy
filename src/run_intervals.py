from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra

from common.ETLSession import ETLSession
from intervals.andersson2014 import ParseAndersson
from intervals.javierre2016 import ParseJavierre
from intervals.jung2019 import ParseJung
from intervals.Liftover import LiftOverSpark
from intervals.thurman2012 import ParseThurman

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:

    etl = ETLSession(cfg)

    # Open and process gene file:
    gene_index = etl.spark.read.parquet(cfg.etl.intervals.inputs.gene_index).persist()

    # Initialize liftover object:
    lift = LiftOverSpark(
        cfg.etl.intervals.inputs.liftover_chain_file,
        cfg.etl.intervals.parameters.max_length_difference,
    )

    # Parsing datasets:
    datasets = [
        # Parsing Andersson data:
        ParseAndersson(
            etl, cfg.etl.intervals.inputs.anderson_file, gene_index, lift
        ).get_intervals(),
        # Parsing Javierre data:
        ParseJavierre(
            etl, cfg.etl.intervals.inputs.javierre_dataset, gene_index, lift
        ).get_intervals(),
        # Parsing Jung data:
        ParseJung(
            etl, cfg.etl.intervals.inputs.jung_file, gene_index, lift
        ).get_intervals(),
        # Parsing Thurman data:
        ParseThurman(
            etl, cfg.etl.intervals.inputs.thurman_file, gene_index, lift
        ).get_intervals(),
    ]

    # Combining all datasets into a single dataframe, where missing columns are filled with nulls:
    df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), datasets)

    etl.logger.info(f"Number of interval data: {df.count()}")
    etl.logger.info(f"Writing data to: {cfg.etl.intervals.outputs.intervals}")

    # Saving data:
    (
        df.orderBy("chrom", "start")
        .repartition(200)
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.intervals.outputs.intervals)
    )


if __name__ == "__main__":

    # Calling parsers:
    main()
