"""Convert and transform LD index for pan-UKBB LD."""

from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import pyspark.sql.functions as f

import gentropy.common.utils
from gentropy.common.session import Session
from gentropy.config import PanUKBBConfig

if TYPE_CHECKING:
    pass


class PanUKBBVariants:
    """Pan-UKBB variants included in the Pan-UKBB LD dataset."""

    def __init__(
        self,
        pan_ukbb_path: str = PanUKBBConfig().pan_ukbb_ht_path,
        pan_ukbb_pops: list[str] = PanUKBBConfig().pan_ukbb_pops,
    ):
        """Initialize.

        Args:
            pan_ukbb_path (str): Path to pan-ukbb hail table.
            pan_ukbb_pops (list[str]): List of populations to include.
        """
        self.pan_ukbb_path = pan_ukbb_path
        self.pan_ukbb_pops = pan_ukbb_pops

    @staticmethod
    def convert_hl_ld_index_to_parquet(
        session: Session,
        path_to_hl_index: str,
        path_parquet_output: str,
        path_chain: str,
        variant_annotation: str,
    ) -> None:
        """Convert and transform LD index for pan-UKBB LD.

        Args:
            session (Session): Session object.
            path_to_hl_index (str): Path to hail table index.
            path_parquet_output (str): Path to save the parquet output.
            path_chain (str): Path to the chain file for lift_over.
            variant_annotation (str): Path to the gnomad variant annotation.
        """
        ht_idx = hl.read_table(path_to_hl_index)
        y = gentropy.common.utils._liftover_loci(
            variant_index=ht_idx,
            chain_path=path_chain,
            dest_reference_genome="GRCh38",
        ).to_spark()

        y = y.withColumnRenamed("locus_GRCh38.contig", "locus_GRCh38_contig")
        y = y.withColumnRenamed("locus_GRCh38.position", "locus_GRCh38_position")

        df = y.withColumn(
            "v1",
            f.concat(
                f.regexp_replace(f.col("locus_GRCh38_contig"), "chr", ""),
                f.lit("_"),
                f.col("locus_GRCh38_position"),
                f.lit("_"),
                f.col("alleles").getItem(0),
                f.lit("_"),
                f.col("alleles").getItem(1),
            ),
        )

        df = df.withColumn(
            "v2",
            f.concat(
                f.regexp_replace(f.col("locus_GRCh38_contig"), "chr", ""),
                f.lit("_"),
                f.col("locus_GRCh38_position"),
                f.lit("_"),
                f.col("alleles").getItem(1),
                f.lit("_"),
                f.col("alleles").getItem(0),
            ),
        )

        df = df.dropDuplicates(["v1"])
        df = df.dropDuplicates(["v2"])

        va = session.spark.read.parquet(variant_annotation)
        va = va.select("variantId")
        va = va.dropDuplicates(["variantId"])

        df_v1_join = df.join(va, df.v1 == va.variantId, how="inner")
        df_v1_join = df_v1_join.withColumn(
            "v1_in_va", f.when(f.col("variantId").isNotNull(), 1).otherwise(0)
        ).select("v1", "v1_in_va")

        # Join df with va on v2 without renaming the column
        df_v2_join = df.join(va, df.v2 == va.variantId, how="inner")
        df_v2_join = df_v2_join.withColumn(
            "v2_in_va", f.when(f.col("variantId").isNotNull(), 1).otherwise(0)
        ).select("v2", "v2_in_va")

        # Combine the results
        result_df = df.join(df_v1_join, on="v1", how="left").join(
            df_v2_join, on="v2", how="left"
        )

        result_df = result_df.withColumnRenamed("v1", "variantId")

        result_df = result_df.withColumn(
            "alleleOrder",
            f.when(
                (f.col("v1_in_va").isNull()) & (f.col("v2_in_va") == 1), -1
            ).otherwise(1),
        )
        result_df = result_df.withColumn(
            "variantId",
            f.when(f.col("alleleOrder") == -1, f.col("v2")).otherwise(
                f.col("variantId")
            ),
        )

        result_df = (
            result_df.select(
                "variantId",
                "idx",
                "locus_GRCh38_contig",
                "locus_GRCh38_position",
                "alleleOrder",
            )
            .orderBy("idx")
            .coalesce(10)
        )

        result_df.write.mode("overwrite").parquet(path_parquet_output)
