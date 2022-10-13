"""Step to generate variant annotation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.json import validate_df_schema
from etl.variants.variant_annotation import generate_variant_annotation


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run variant annotation generation."""
    etl = ETLSession(cfg)
    etl.logger.info("Generating variant annotation...")

    variants = generate_variant_annotation(
        etl,
        cfg.etl.variant_annotation.inputs.gnomad_file,
        cfg.etl.variant_annotation.inputs.chain_file,
    )

    validate_df_schema(variants, "variant_annotation.json")

    etl.logger.info("Variant annotation converted to Spark DF. Saving...")
    # Writing data partitioned by chromosome and position:
    (
        variants.repartition("chromosome")
        .orderBy("position")
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.variant_annotation.outputs.variant_annotation)
    )


if __name__ == "__main__":

    main()
