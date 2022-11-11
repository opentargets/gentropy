"""Variant index generation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.v2g.distance.distance import main as v2g_distance


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run variant index generation."""
    etl = ETLSession(cfg)

    v2g_distance_df = v2g_distance(
        etl,
        cfg.etl.v2g.inputs.variant_index,
        cfg.etl.v2g.inputs.gene_index,
        cfg.etl.v2g.parameters.tss_distance_threshold,
    ).repartition(400)

    etl.logger.info(f"Writing V2G evidence to: {cfg.etl.v2g.outputs.v2g_distance}")

    v2g_distance_df.write.partitionBy("chromosome").mode(
        cfg.environment.sparkWriteMode
    ).parquet(cfg.etl.v2g.outputs.v2g_distance)


if __name__ == "__main__":

    main()
