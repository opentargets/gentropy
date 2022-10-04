from __future__ import annotations

from typing import TYPE_CHECKING

import hydra
import pyspark.sql.functions as f

if TYPE_CHECKING:
    from omegaconf import DictConfig

from etl.common.ETLSession import ETLSession
from etl.variant_index.variant_index import (
    calculate_dist_to_gene,
    get_variants_intersect,
)
from src.etl.common.utils import get_gene_tss


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:

    etl = ETLSession(cfg)

    etl.logger.info("Generating variant index...")

    # Extract what are the nearest genes (protein coding and of other types) to each variant
    gene_idx = (
        # TODO READ GENE INDEX schema
        etl.spark.read.parquet(cfg.variant_index.inputs.gene_index)
        .withColumn(
            "tss",
            get_gene_tss(
                f.col("genomicLocation.strand"),
                f.col("genomicLocation.start"),
                f.col("genomicLocation.end"),
            ),
        )
        .select(
            f.col("id").alias("geneId"), "genomicLocation.chromosome", "tss", "biotype"
        )
    )
    coding_gene_distances = calculate_dist_to_gene(
        gene_idx.filter(f.col("biotype") == "protein_coding"),
        cfg.variant_index.coding_gene_dist_path,
        cfg.variant_index.parameters.tss_distance_threshold,
    ).selectExpr(
        "variantId",
        "geneId as geneIdAny",
        "distance as geneIdProtCodingDistance",
    )
    any_gene_distances = calculate_dist_to_gene(
        gene_idx,
        cfg.variant_index.any_gene_dist_path,
        cfg.variant_index.parameters.tss_distance_threshold,
    ).selectExpr(
        "variantId",
        "geneId as geneIdProtCoding",
        "distance as geneIdAnyDistance",
    )
    distances = coding_gene_distances.join(
        any_gene_distances, on="variantId", how="outer"
    ).withColumnRenamed("variantId", "id")

    variants = (
        get_variants_intersect(
            etl,
            cfg.etl.variant_index.inputs.variant_annotation,
            cfg.etl.variant_index.inputs.credible_sets,
            cfg.etl.variant_index.parameters.partition_count,
        )
        .join(distances, on="id", how="left")
        .dropDuplicates(["id"])
    )

    etl.logger.info(f"Writing data to: {cfg.etl.variant_index.outputs.variant_index}")

    # TODO: write ordered output
    (
        variants.write.mode(cfg.environment.sparkWriteMode).parquet(
            cfg.etl.variant_index.outputs.variant_index
        )
    )


if __name__ == "__main__":

    main()
