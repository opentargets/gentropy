from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra

from etl.common.ETLSession import ETLSession
from etl.intervals.andersson2014 import ParseAndersson
from etl.intervals.helpers import prepare_gene_interval_lut
from etl.intervals.javierre2016 import ParseJavierre
from etl.intervals.jung2019 import ParseJung
from etl.intervals.Liftover import LiftOverSpark
from etl.intervals.thurman2012 import ParseThurman
from etl.json import validate_df_schema

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:

    etl = ETLSession(cfg)

    # Open and process gene file:
    gene_index = prepare_gene_interval_lut(
        etl.read_parquet(cfg.etl.intervals.inputs.gene_index, "targets.json")
    )

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

    validate_df_schema(df, "intervals.json")

    # Saving data:
    (
        df.repartitionByRange("chromosome", "start")
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.intervals.outputs.intervals)
    )


if __name__ == "__main__":

    # Calling parsers:
    main()
