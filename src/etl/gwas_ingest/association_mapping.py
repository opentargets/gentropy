"""Functions to reliable map GWAS Catalog associations to GnomAD3.1 variants."""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as f
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from etl.common.ETLSession import ETLSession


def map_variants(
    parsed_associations: DataFrame, variant_annotation_path: str, etl: ETLSession
) -> DataFrame:
    """Add variant metadata in associations.

    Args:
        parsed_associations (DataFrame): associations
        etl (ETLSession): current ETL session
        variant_annotation_path (str): variant annotation path

    Returns:
        DataFrame: associations with variant metadata
    """
    variants = etl.spark.read.parquet(variant_annotation_path).select(
        f.col("id").alias("variantId"),
        f.col("chromosome"),
        f.col("position").alias("position"),
        f.col("rsIds").alias("rsIdsGnomad"),
        f.col("referenceAllele"),
        f.col("alternateAllele"),
        f.col("alleleFrequencies"),
    )

    mapped_associations = (
        variants.join(
            f.broadcast(parsed_associations), on=["chromosome", "position"], how="right"
        )
        # Even if there's no variant mapping in gnomad, to make sure we are not losing any assocations,
        .withColumn("variantId", f.coalesce(f.col("variantId"), f.col("gwasVariant")))
        # Flagging variants that could not be mapped to gnomad:
        .withColumn(
            "qualityControl",
            f.when(
                f.col("alternateAllele").isNull(),
                f.array_union(
                    f.col("qualityControl"), f.array(f.lit("No mapping in GnomAd"))
                ),
            ).otherwise(f.col("qualityControl")),
        ).persist()
    )
    assoc_without_variant = mapped_associations.filter(
        f.col("referenceAllele").isNull()
    ).count()
    etl.logger.info(
        f"Loading variant annotation and joining with associations... {assoc_without_variant} associations outside gnomAD"
    )
    return mapped_associations


def _check_rsids(gnomad: Column, gwas: Column) -> Column:
    """If the intersection of the two arrays is greater than 0, return True, otherwise return False.

    Args:
        gnomad (Column): rsids from gnomad
        gwas (Column): rsids from the GWAS Catalog

    Returns:
        A boolean column that is true if the GnomAD rsIDs can be found in the GWAS rsIDs.
    """
    return f.when(f.size(f.array_intersect(gnomad, gwas)) > 0, True).otherwise(False)


def _find_mappings_to_drop(association_id: Column, filter_column: Column) -> Column:
    """Flagging mappings to drop for each association.

    Some associations have multiple mappings. Some has matching rsId others don't. We only
    want to drop the non-matching mappings, when a matching is available for the given association.
    This logic can be generalised for other measures eg. allele concordance.

    Args:
        association_id (Column): association identifier column
        filter_column (Column): boolean col indicating to keep a mapping

    Returns:
        A column with a boolean value.
    """
    w = Window.partitionBy(association_id)

    # Generating a boolean column informing if the filter column contains true anywhere for the association:
    aggregated_filter = f.when(
        f.array_contains(f.collect_set(filter_column).over(w), True), True
    ).otherwise(False)

    # Generate a filter column:
    return f.when(aggregated_filter & (~filter_column), False).otherwise(True)


def _keep_mapping_with_top_maf(association_id: Column, maf_column: Column) -> Column:
    """For each association ID, keep the row with the highest MAF.

    Args:
        association_id (Column): Column
        maf_column (Column): the column that contains the MAF values

    Returns:
        A column with a boolean value.
    """
    w = Window.partitionBy(association_id).orderBy(f.desc(maf_column))
    row_numbers = f.row_number().over(w)
    return f.when(row_numbers == 1, True).otherwise(False)


def _check_concordance(
    risk_allele: Column, reference_allele: Column, alternate_allele: Column
) -> Column:
    """A function to check if the risk allele is concordant with the alt or ref allele.

    If the risk allele is the same as the reference or alternate allele, or if the reverse complement of
    the risk allele is the same as the reference or alternate allele, then the allele is concordant

    Args:
        risk_allele (Column): The allele that is associated with the risk of the disease.
        reference_allele (Column): The reference allele from the GWAS catalog
        alternate_allele (Column): The alternate allele of the variant.

    Returns:
        A boolean column that is True if the risk allele is the same as the reference or alternate allele,
        or if the reverse complement of the risk allele is the same as the reference or alternate allele.
    """
    # Calculating the reverse complement of the risk allele:
    risk_allele_reverse_complement = f.when(
        risk_allele.rlike(r"^[ACTG]+$"),
        f.reverse(f.translate(risk_allele, "ACTG", "TGAC")),
    ).otherwise(risk_allele)

    # OK, is the risk allele or the reverse complent is the same as the mapped alleles:
    return (
        f.when(
            (risk_allele == reference_allele) | (risk_allele == alternate_allele),
            True,
        )
        # If risk allele is found on the negative strand:
        .when(
            (risk_allele_reverse_complement == reference_allele)
            | (risk_allele_reverse_complement == alternate_allele),
            True,
        )
        # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
        .when(risk_allele == "?", True)
        # If the association could not be mapped we keep it:
        .when(reference_allele.isNull(), True)
        # Allele is discordant:
        .otherwise(False)
    )


def clean_mappings(df: DataFrame) -> DataFrame:
    """A function to sort out multiple mappings for associations.

    We keep the mapping with the highest MAF, and if there are multiple mappings with the same MAF, we
    keep the one with the matching rsId, and if there are multiple mappings with the same MAF and rsId,
    we keep the one with concordant alleles

    Args:
        df (DataFrame): DataFrame

    Returns:
        A dataframe with the following columns:
        - associationId
        - variantId
        - rsIdsGnomad
        - rsIdsGwasCatalog
        - riskAllele
        - referenceAllele
        - alternateAllele
        - alleleFrequencies
        - isRsIdMatched
        - isConcordant
    """
    all_mappings = df.withColumn(
        "isRsIdMatched", _check_rsids(f.col("rsIdsGnomad"), f.col("rsIdsGwasCatalog"))
    ).withColumn(
        "isConcordant",
        _check_concordance(
            f.col("riskAllele"), f.col("referenceAllele"), f.col("alternateAllele")
        ),
    )

    mafs = (
        df.select("variantId", f.explode(f.col("alleleFrequencies")).alias("af"))
        .distinct()
        .withColumn(
            "maf",
            f.when(
                f.col("af.alleleFrequency") > 0.5, 1 - f.col("af.alleleFrequency")
            ).otherwise(f.col("af.alleleFrequency")),
        )
        .groupBy("variantId")
        .agg(f.max("maf").alias("maxMaf"))
    )

    return (
        all_mappings.join(mafs, on="variantId", how="left")
        # Dropping rows, where rsId doesn't match, but matching rsId available:
        .withColumn(
            "rsidFilter",
            _find_mappings_to_drop(f.col("associationId"), f.col("isRsIdMatched")),
        )
        .filter(f.col("rsidFilter"))
        # Dropping rows, where alleles aren't concordant, but concordant alleles available:
        .withColumn(
            "concordanceFilter",
            _find_mappings_to_drop(f.col("associationId"), f.col("isConcordant")),
        )
        .filter(f.col("concordanceFilter"))
        # Out of the remaining mappings, keeping the one with the highest MAF:
        .withColumn(
            "mafFilter",
            _keep_mapping_with_top_maf(f.col("associationId"), f.col("maxMaf")),
        )
        .filter("mafFilter")
        .drop(
            "rsidFilter",
            "concordanceFilter",
            "mafFilter",
            "isRsIdMatched",
            "maxMaf",
            "isRsIdMatched",
            "isConcordant",
        )
        .persist()
    )
