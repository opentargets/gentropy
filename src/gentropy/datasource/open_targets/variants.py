"""Process OT dataset with variant information."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.spark import (
    create_empty_column_if_not_exists,
    safe_array_union,
)
from gentropy.dataset.study_locus import StudyLocus
from gentropy.datasource.ensembl.api import fetch_coordinates_from_rsids

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy.common.session import Session


class OpenTargetsVariant:
    """Process OT dataset with variant information."""

    @classmethod
    def map_rsids_to_variant_ids(
        cls: type[OpenTargetsVariant],
        session: Session,
        variant_df: DataFrame,
    ) -> DataFrame:
        """Map rsIDs to variant IDs where variantId is null.

        Args:
            session (Session): Spark session.
            variant_df (DataFrame): DataFrame with variant information.

        Returns:
            DataFrame: DataFrame with mapped variant IDs.
        """
        if rsids_to_map := (
            variant_df.filter(
                (f.col("variantId").isNull()) & (f.col("variantRsId").isNotNull())
            )
            .select("variantRsId")
            .distinct()
            .toPandas()["variantRsId"]
            .to_list()
        ):
            rsid_to_variantids = fetch_coordinates_from_rsids(rsids_to_map)
            mapping_df = session.spark.createDataFrame(
                rsid_to_variantids.items(), schema=["variantRsId", "mappedVariantIds"]
            ).select(
                "variantRsId", f.explode("mappedVariantIds").alias("mappedVariantId")
            )

            variant_df = (
                variant_df.join(mapping_df, "variantRsId", "left")
                .withColumn(
                    "variantId",
                    f.coalesce(f.col("variantId"), f.col("mappedVariantId")),
                )
                .drop("mappedVariantId")
            )

        return variant_df

    @classmethod
    def as_vcf_df(
        cls: type[OpenTargetsVariant],
        session: Session,
        variant_df: DataFrame,
    ) -> DataFrame:
        """Convert OT dataset to VCF format. VCF format is widely used and is compatible with most variant annotation tools, including VEP.

        Args:
            session (Session): Spark session.
            variant_df (DataFrame): DataFrame with variant information.

        Returns:
            DataFrame: DataFrame with variant information in VCF format.
        """
        # Add necessary cols if not present and apply rsID mappings
        mandatory_cols = ["variantId", "variantRsId", "locus"]
        missing_cols = [col for col in mandatory_cols if col not in variant_df.columns]
        for col in missing_cols:
            if col == "locus":
                variant_df = variant_df.withColumn(
                    col,
                    create_empty_column_if_not_exists(
                        col,
                        StudyLocus.get_schema()["locus"].dataType,
                    ),
                )
            else:
                variant_df = variant_df.withColumn(
                    col, create_empty_column_if_not_exists(col)
                )
        return (
            variant_df.filter(f.col("variantId").isNotNull())
            .withColumn(
                # Combine variant IDs from variantId and locus.variantId
                "variantId",
                f.explode(
                    safe_array_union(
                        f.array(f.col("variantId")),
                        f.col("locus.variantId"),
                    )
                ),
            )
            .select(
                f.coalesce(f.split(f.col("variantId"), "_")[0], f.lit(".")).alias(
                    "#CHROM"
                ),
                f.coalesce(f.split(f.col("variantId"), "_")[1], f.lit("."))
                .cast("int")
                .alias("POS"),
                f.coalesce(f.col("variantRsId"), f.lit(".")).alias("ID"),
                f.coalesce(f.split(f.col("variantId"), "_")[2], f.lit(".")).alias(
                    "REF"
                ),
                f.coalesce(f.split(f.col("variantId"), "_")[3], f.lit(".")).alias(
                    "ALT"
                ),
                f.lit(".").alias("QUAL"),
                f.lit(".").alias("FILTER"),
                f.lit(".").alias("INFO"),
            )
            .distinct()
            .filter(
                (f.col("#CHROM") != ".")
                & (f.col("POS").isNotNull())
                & (f.col("REF").rlike("^[GCTA.]*$"))
                & (f.col("ALT").rlike("^[GCTA.]*$"))
            )
            .orderBy(f.col("#CHROM").asc(), f.col("POS").asc())
        )
