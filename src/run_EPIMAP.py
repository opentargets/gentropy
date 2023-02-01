"""Step to liftover on big epimap annotation."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.tissue_enrichment.EPIMAP import ParseEPIMAP
from etl.v2g.intervals.Liftover import LiftOverSpark


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run liftover on big epimap annotation matrix."""
    etl = ETLSession(cfg)

    etl.logger.info("Lifting over EPIMAP annotation...")

    lift = LiftOverSpark(
        cfg.etl.v2g.inputs.liftover_chain_file,
        10000000,
    )

    epimap_hg38 = ParseEPIMAP(
        etl, cfg.etl.tissue_enrichment.inputs.tissue_annotations, lift
    ).get_intervals()
    epimap_hg38.write.mode(cfg.environment.sparkWriteMode).parquet(
        cfg.etl.tissue_enrichment.outputs.annotations_hg38
    )


if __name__ == "__main__":

    main()
