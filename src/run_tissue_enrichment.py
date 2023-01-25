"""Calculate tissue enrichment."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

from etl.common.ETLSession import ETLSession
from etl.tissue_enrichment.tissue_enrichment import cheers
from etl.tissue_enrichment.utils import load_peaks, load_snps

if TYPE_CHECKING:
    from omegaconf import DictConfig

# Import required functions from src :
# from etl.common.ETLSession import ETLSession
# from etl.json import validate_df_schema
# from etl.variants.variant_annotation import generate_variant_annotation


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run tissue enrichment analysis using the CHEERS method."""
    etl = ETLSession(cfg)
    etl.logger.info("Loading data for tissue enrichment calculations...")

    # Loading data

    summary_stats_credible_set = load_snps(
        etl.spark, cfg.etl.tissue_enrichment.inputs.summary_stats_credible_set
    )

    #    pics_credible_set = load_snps(
    #        etl.spark, cfg.etl.tissue_enrichment.inputs.pics_credible_set
    #    )
    tissue_annotations = load_peaks(
        etl.spark, cfg.etl.tissue_enrichment.inputs.tissue_annotations
    )

    etl.logger.info("Computing tissue enrichment calculations...")

    summary_stats_enrichment = cheers(tissue_annotations, summary_stats_credible_set)

    #    pics_enrichment = cheers(
    #        tissue_annotations,
    #        pics_credible_set,
    #    )

    etl.logger.info("Writing tissue enrichment results...")
    (
        summary_stats_enrichment.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.tissue_enrichment.outputs.summary_stats_enrichment
        )
    )

    #    (
    #        pics_enrichment.write.mode(cfg.environment.sparkWriteMode).parquet(
    #            cfg.etl.tissue_enrichment.outputs.pics_enrichment
    #        )
    #    )

    etl.logger.info("Tissue enrichment finished")


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
