"""Variant index generation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.variants.variant_index import main as generate_variant_index


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run variant index generation."""
    etl = ETLSession(cfg)

    variant_idx, invalid_variants = generate_variant_index(
        etl,
        cfg.etl.variant_index.inputs.variant_annotation,
        cfg.etl.variant_index.inputs.credible_sets,
        cfg.etl.variant_index.inputs.gene_index,
        cfg.etl.variant_index.parameters.partition_count,
        cfg.etl.variant_index.parameters.tss_distance_threshold,
    )

    etl.logger.info(
        f"Writing invalid variants from the credible set to: {cfg.etl.variant_index.outputs.variant_invalid}"
    )
    invalid_variants.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.variant_index.outputs.variant_invalid
    )
    etl.logger.info(
        f"Writing variant index to: {cfg.etl.variant_index.outputs.variant_index}"
    )
    # TODO - validate the output
    print(variant_idx.schema.jsonValue())
    variant_idx.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.variant_index.outputs.variant_index
    )


if __name__ == "__main__":

    main()
