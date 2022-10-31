"""Step to aggregate variant-to-gene assesments."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.v2g.functional_predictions.vep import main as extract_v2g_from_vep
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

    etl.logger.info(f"Writing all V2G evidence to: {cfg.etl.v2g.outputs.v2g}")
    interval_datasets = [
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
    ]
    func_pred_datasets = extract_v2g_from_vep(
        # Parsing VEP functional consequences
        etl,
        cfg.etl.v2g.inputs.variant_index,
        cfg.etl.v2g.inputs.variant_annotation,
        cfg.etl.v2g.inputs.vep_consequences,
    )
    datasets = interval_datasets + list(func_pred_datasets)
    v2g = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), datasets)

    # TODO: validate output
    print(v2g.schema.jsonValue())
    # validate_df_schema(df, "v2g.json")
    (
        v2g.coalesce(cfg.etl.v2g.parameters.partition_count)
        .select(
            "*",
            f.split("variantId", "_")[0].alias("chromosome"),
            f.split("variantId", "_")[1].alias("position"),
        )
        .orderBy("position")
        .write.partitionBy("chromosome")
        .mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.v2g.outputs.v2g)
    )
    etl.logger.info("V2G set generation complete.")


if __name__ == "__main__":

    main()
