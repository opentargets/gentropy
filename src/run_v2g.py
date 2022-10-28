"""Step to aggregate variant-to-gene assesments."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.v2g.functional_predictions.vep import main as extract_v2g_consequence
from etl.v2g.intervals.andersson2014 import ParseAndersson
from etl.v2g.intervals.helpers import (
    get_variants_in_intervals,
    prepare_gene_interval_lut,
)
from etl.v2g.intervals.javierre2016 import ParseJavierre
from etl.v2g.intervals.jung2019 import ParseJung
from etl.v2g.intervals.Liftover import LiftOverSpark
from etl.v2g.intervals.thurman2012 import ParseThurman


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run V2G set generation."""
    etl = ETLSession(cfg)

    etl.logger.info("Generating V2G evidence from interval data...")
    gene_index = prepare_gene_interval_lut(
        etl.read_parquet(cfg.etl.intervals.inputs.gene_index, "targets.json")
    )
    vi = etl.read_parquet(
        cfg.etl.v2g.inputs.variant_index, "variant_index.json"
    ).selectExpr("id as variantId", "chromosome", "position")
    lift = LiftOverSpark(
        cfg.etl.intervals.inputs.liftover_chain_file,
        cfg.etl.intervals.parameters.max_length_difference,
    )

    etl.logger.info(f"Writing all V2G evidence to: {cfg.etl.v2g.outputs.vep}")
    datasets = [
        # Parsing Andersson data:
        ParseAndersson(etl, cfg.etl.intervals.inputs.anderson_file, gene_index, lift)
        .get_intervals()
        .transform(lambda df: get_variants_in_intervals(df, vi)),
        # Parsing Javierre data:
        ParseJavierre(etl, cfg.etl.intervals.inputs.javierre_dataset, gene_index, lift)
        .get_intervals()
        .transform(lambda df: get_variants_in_intervals(df, vi)),
        # Parsing Jung data:
        ParseJung(etl, cfg.etl.intervals.inputs.jung_file, gene_index, lift)
        .get_intervals()
        .transform(lambda df: get_variants_in_intervals(df, vi)),
        # Parsing Thurman data:
        ParseThurman(etl, cfg.etl.intervals.inputs.thurman_file, gene_index, lift)
        .get_intervals()
        .transform(lambda df: get_variants_in_intervals(df, vi)),
        # Parsing VEP functional consequences:
        extract_v2g_consequence(
            etl,
            cfg.etl.v2g.inputs.variant_index,
            cfg.etl.v2g.inputs.variant_annotation,
            cfg.etl.v2g.inputs.vep_consequences,
        ),
    ]
    v2g = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), datasets)

    # TODO: validate output
    # validate_df_schema(df, "intervals.json")
    v2g.write.mode(cfg.environment.sparkWriteMode).parquet(cfg.etl.v2g.outputs.vep)


if __name__ == "__main__":

    main()
