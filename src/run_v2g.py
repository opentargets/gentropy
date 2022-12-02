"""Step to aggregate variant-to-gene assesments."""
from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.json import validate_df_schema
from etl.v2g.functional_predictions.vep import main as extract_v2g_from_vep
from etl.v2g.intervals.andersson2014 import ParseAndersson
from etl.v2g.intervals.helpers import (
    get_variants_in_interval,
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
        etl.read_parquet(cfg.etl.v2g.inputs.gene_index, "targets.json")
    ).persist()
    vi = (
        etl.read_parquet(cfg.etl.v2g.inputs.variant_index, "variant_index.json")
        .selectExpr("id as variantId", "chromosome", "position")
        .persist()
    )
    va = etl.read_parquet(
        cfg.etl.v2g.inputs.variant_annotation, "variant_annotation.json"
    ).selectExpr("id as variantId", "chromosome", "vep")
    lift = LiftOverSpark(
        cfg.etl.v2g.inputs.liftover_chain_file,
        cfg.etl.v2g.parameters.liftover_max_length_difference,
    )

    interval_datasets = [
        ParseAndersson(
            etl, cfg.etl.v2g.inputs.anderson_file, gene_index, lift
        ).get_intervals(),
        ParseJavierre(
            etl, cfg.etl.v2g.inputs.javierre_dataset, gene_index, lift
        ).get_intervals(),
        ParseJung(etl, cfg.etl.v2g.inputs.jung_file, gene_index, lift).get_intervals(),
        ParseThurman(
            etl, cfg.etl.v2g.inputs.thurman_file, gene_index, lift
        ).get_intervals(),
    ]
    interval_df = reduce(
        lambda x, y: x.unionByName(y, allowMissingColumns=True), interval_datasets
    ).transform(lambda df: get_variants_in_interval(df, vi))
    func_pred_datasets = extract_v2g_from_vep(
        etl,
        vi,
        va,
        cfg.etl.v2g.inputs.vep_consequences,
    )
    v2g = (
        reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=True),
            [interval_df, *func_pred_datasets],
        )
        # V2G assignments are restricted to a relevant set of genes (mostly protein coding)
        .join(
            gene_index.filter(
                f.col("biotype").isin(list(cfg.etl.v2g.parameters.approved_biotypes))
            ).select("geneId"),
            on="geneId",
            how="inner",
        ).distinct()
    )
    validate_df_schema(v2g, "v2g.json")
    (
        v2g.repartition(cfg.etl.v2g.parameters.partition_count, "chromosome")
        .withColumn("position", f.split(f.col("variantId"), "_")[1])
        .sortWithinPartitions("chromosome", "position")
        .write.partitionBy("chromosome")
        .mode(cfg.environment.sparkWriteMode)
        .parquet(cfg.etl.v2g.outputs.v2g)
    )

    etl.logger.info(f"V2G set has been written to {cfg.etl.v2g.outputs.v2g}.")


if __name__ == "__main__":

    main()
