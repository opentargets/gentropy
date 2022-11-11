"""Step to aggregate variant-to-gene assesments."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from pyspark.sql import DataFrame

from etl.common.ETLSession import ETLSession
from etl.json import validate_df_schema
from etl.v2g.functional_predictions.vep import main as extract_v2g_from_vep
from etl.v2g.intervals.helpers import get_variants_in_interval


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run V2G set generation."""
    etl = ETLSession(cfg)

    etl.logger.info("Generating V2G evidence from interval data...")
    vi = (
        etl.read_parquet(cfg.etl.v2g.inputs.variant_index, "variant_index.json")
        .selectExpr("id as variantId", "chromosome", "position")
        .repartition("chromosome")
        .persist()
    )

    interval_df = etl.read_parquet(
        cfg.etl.v2g.inputs.interval_data, "intervals.json"
    ).transform(lambda df: get_variants_in_interval(df, vi))

    func_pred_df = extract_v2g_from_vep(
        etl,
        cfg.etl.v2g.inputs.variant_index,
        cfg.etl.v2g.inputs.variant_annotation,
        cfg.etl.v2g.inputs.vep_consequences,
    )

    def export_v2g_df(df: DataFrame, name: str) -> None:
        """Export each type of V2G DataFrame to parquet partitioned and ordered."""
        validate_df_schema(df, "v2g.json")
        (
            df.coalesce(
                cfg.etl.v2g.parameters.partition_count,
            )
            # .sortWithinPartitions("chromosome", "position")
            .drop("position")
            .write.partitionBy(*cfg.etl.v2g.parameters.partition_columns)
            .mode(cfg.environment.sparkWriteMode)
            .parquet(f"{cfg.etl.v2g.outputs.v2g}/{name}")
        )
        etl.logger.info(f"{name} V2G saved.")

    v2g_datasets = [("intervals", interval_df), ("vep", func_pred_df)]
    for name, df in v2g_datasets:
        export_v2g_df(df, name)


if __name__ == "__main__":

    main()
