"""Variant index generation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra
from pyspark.sql import functions as f

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.variants.variant_index import join_variants_w_credset


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run variant index generation."""
    etl = ETLSession(cfg)

    variants_df = join_variants_w_credset(
        etl,
        cfg.etl.variant_index.inputs.variant_annotation,
        cfg.etl.variant_index.inputs.credible_sets,
        cfg.etl.variant_index.parameters.partition_count,
    ).persist()

    etl.logger.info(
        f"Writing invalid variants from the credible set to: {cfg.etl.variant_index.outputs.variant_invalid}"
    )
    variants_df.filter(~f.col("variantInGnomad")).write.mode(
        cfg.environment.sparkWriteMode
    ).parquet(cfg.etl.variant_index.outputs.variant_invalid)
    etl.logger.info(
        f"Writing variant index to: {cfg.etl.variant_index.outputs.variant_index}"
    )
    # TODO - validate the output
    print(variants_df.drop("variantInGnomad").schema.jsonValue())
    variants_df.filter(f.col("variantInGnomad")).drop("variantInGnomad").write.mode(
        cfg.environment.sparkWriteMode
    ).parquet(cfg.etl.variant_index.outputs.variant_index)


if __name__ == "__main__":

    main()
