"""Step to aggregate variant-to-gene assesments."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.v2g.functional_predictions.vep import main as extract_v2g_consequence


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run V2G set generation."""
    etl = ETLSession(cfg)

    # TODO: call interval parsers here
    etl.logger.info("Generating V2G evidence from VEP...")
    vep = extract_v2g_consequence(
        etl,
        cfg.etl.v2g.inputs.variant_index,
        cfg.etl.v2g.inputs.variant_annotation,
        cfg.etl.v2g.inputs.vep_consequences,
    )

    etl.logger.info(f"Writing V2G evidence from VEP to: {cfg.etl.v2g.outputs.vep}")
    # TODO: validate output
    vep.write.mode(cfg.environment.sparkWriteMode).parquet(cfg.etl.v2g.outputs.vep)


if __name__ == "__main__":

    main()
