"""Step to parse interval data."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra

from etl.common.ETLSession import ETLSession
from etl.json import validate_df_schema
from etl.v2g.intervals.andersson2014 import ParseAndersson
from etl.v2g.intervals.helpers import prepare_gene_interval_lut
from etl.v2g.intervals.javierre2016 import ParseJavierre
from etl.v2g.intervals.jung2019 import ParseJung
from etl.v2g.intervals.Liftover import LiftOverSpark
from etl.v2g.intervals.thurman2012 import ParseThurman

if TYPE_CHECKING:
    from omegaconf import DictConfig


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run interval parsers."""
    etl = ETLSession(cfg)

    etl.logger.info("Parsing interval data...")
    gene_index = prepare_gene_interval_lut(
        etl.read_parquet(cfg.etl.intervals.inputs.gene_index, "targets.json")
    ).persist()
    lift = LiftOverSpark(
        cfg.etl.intervals.inputs.liftover_chain_file,
        cfg.etl.intervals.parameters.liftover_max_length_difference,
    )

    interval_datasets = [
        ParseAndersson(
            etl, cfg.etl.intervals.inputs.anderson_file, gene_index, lift
        ).get_intervals(),
        ParseJavierre(
            etl, cfg.etl.intervals.inputs.javierre_dataset, gene_index, lift
        ).get_intervals(),
        ParseJung(
            etl, cfg.etl.intervals.inputs.jung_file, gene_index, lift
        ).get_intervals(),
        ParseThurman(
            etl, cfg.etl.intervals.inputs.thurman_file, gene_index, lift
        ).get_intervals(),
    ]

    interval_df = reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), interval_datasets
    )

    etl.logger.info(f"Writing data to: {cfg.etl.intervals.outputs.interval_data}")

    validate_df_schema(interval_df, "intervals.json")

    # Saving data:
    (
        interval_df.repartition(
            cfg.etl.intervals.parameters.partition_count,
            *cfg.etl.intervals.parameters.partition_columns,
        )
        .sortWithinPartitions("chromosome", "start")
        .write.partitionBy(*cfg.etl.intervals.parameters.partition_columns)
        .mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.intervals.outputs.interval_data)
    )


if __name__ == "__main__":
    main()
