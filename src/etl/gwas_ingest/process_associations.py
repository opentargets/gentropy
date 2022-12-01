"""Process GWAS catalog associations."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

from etl.gwas_ingest.effect_harmonization import harmonize_effect
from etl.gwas_ingest.study_ingestion import parse_efos
from etl.json import data

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from etl.common.ETLSession import ETLSession

ASSOCIATION_COLUMNS_MAP = {
    # Variant related columns:
    "STRONGEST SNP-RISK ALLELE": "strongestSnpRiskAllele",  # variant id and the allele is extracted (; separated list)
    "CHR_ID": "chromosome",  # Mapped genomic location of the variant (; separated list)
    "CHR_POS": "position",
    "RISK ALLELE FREQUENCY": "riskAlleleFrequency",
    "CNV": "cnv",  # Flag if a variant is a copy number variant
    "SNP_ID_CURRENT": "snpIdCurrent",  #
    "SNPS": "snpIds",  # List of all SNPs associated with the variant
    # Study related columns:
    "STUDY ACCESSION": "studyAccession",
    # Disease/Trait related columns:
    "DISEASE/TRAIT": "associationDiseaseTrait",  # Reported trait of the study
    "MAPPED_TRAIT_URI": "mappedTraitUri",  # Mapped trait URIs of the study
    # "MERGED": "merged",
    "P-VALUE (TEXT)": "pValueText",  # Extra information on the association.
    # Association details:
    "P-VALUE": "pValue",  # p-value of the association, string: split into exponent and mantissa.
    "PVALUE_MLOG": "pValueNeglog",  # -log10(p-value) of the association, float # No longer needed. Can be calculated.
    "OR or BETA": "effectSize",  # Effect size of the association, Odds ratio or beta
    "95% CI (TEXT)": "confidenceInterval",  # Confidence interval of the association, string: split into lower and upper bound.
    # "CONTEXT": "context",
}


@f.udf(t.ArrayType(t.StringType(), containsNull=True))
def _clean_pvaluetext(s: str) -> list[str] | None:
    """User defined function for PySpark to clean comma separated texts.

    Args:
        s (str): str

    Returns:
        A list of strings.
    """
    if s:
        return [w.strip() for w in s.split(",")]
    else:
        return []


def _parse_pvaluetext_ancestry(pvaluetext: Column) -> Column:
    """Parsing ancestry information captured in p-value text.

    If the pValueText column contains the word "uropean", then return "nfe". If it contains the word
    "frican", then return "afr"

    Args:
        pvaluetext (Column): The column that contains the parsed p-value text

    Returns:
        A column with the values "nfe" or "afr"
    """
    return f.when(pvaluetext.contains("uropean"), "nfe").when(
        pvaluetext.contains("frican"), "afr"
    )


def _pvalue_text_resolve(df: DataFrame, etl: ETLSession) -> DataFrame:
    """Parsind p-value text.

    Parsing `pValueText` column + mapping abbreviations reference list.
    `pValueText` that has been cleaned and resolved to a standardised format.
    `pValueAncestry` column with parsed ancestry information

    Args:
        df (DataFrame): DataFrame
        etl (ETLSession): ETLSession

    Returns:
        A dataframe with the p-value text resolved.
    """
    columns = df.columns

    # GWAS Catalog to p-value mapping
    pval_mapping = etl.spark.createDataFrame(
        json.loads(
            pkg_resources.read_text(
                data, "gwas_pValueText_resolve.json", encoding="utf-8"
            )
        )
    ).withColumn("full_description", f.lower(f.col("full_description")))

    # Explode pvalue-mappings:
    return (
        df
        # Cleaning and fixing p-value text:
        .withColumn(
            "pValueText",
            _clean_pvaluetext(f.regexp_replace(f.col("pValueText"), r"[\(\)]", "")),
        )
        .select(
            # Exploding p-value text:
            *[
                f.explode_outer(col).alias(col) if col == "pValueText" else col
                for col in columns
            ]
        )
        # Join with mapping:
        .join(pval_mapping, on="pValueText", how="left")
        # Coalesce mappings:
        .withColumn("pValueText", f.coalesce("full_description", "pValueText"))
        # Resolve ancestries:
        .withColumn("pValueAncestry", _parse_pvaluetext_ancestry(f.col("pValueText")))
        # Aggregate data:
        .groupBy(*[col for col in columns if col != "pValueText"])
        .agg(
            f.collect_set(f.col("pValueAncestry")).alias("pValueAncestry"),
            f.collect_set(f.col("pValueText")).alias("pValueText"),
        )
        .withColumn("pValueText", f.concat_ws(", ", f.col("pValueText")))
        .withColumn("pValueAncestry", f.concat_ws(", ", f.col("pValueAncestry")))
    )


def read_associations_data(
    etl: ETLSession, gwas_association_file: str, pvalue_cutoff: float
) -> DataFrame:
    """Read GWASCatalog associations.

    It reads the GWAS Catalog association dataset, selects and renames columns, casts columns, and
    flagges associatinos that does not reach GWAS significance level.

    Args:
        etl (ETLSession): current ETL session
        gwas_association_file (str): path to association file CSV
        pvalue_cutoff (float): association p-value cut-off

    Returns:
        DataFrame: `DataFrame` with the GWAS Catalog associations
    """
    etl.logger.info("Starting ingesting GWAS Catalog associations...")

    # Reading and filtering associations:
    association_df = (
        etl.spark.read.csv(gwas_association_file, sep="\t", header=True)
        # Select and rename columns:
        .select(
            *[
                f.col(old_name).alias(new_name)
                for old_name, new_name in ASSOCIATION_COLUMNS_MAP.items()
            ],
            f.monotonically_increasing_id().alias("associationId"),
        )
        # Based on pre-defined filters set flag for failing associations:
        # 1. Flagging associations based on variant x variant interactions
        # 2. Flagging sub-significant associations
        # 3. Flagging associations without genomic location
        .withColumn(
            "qualityControl",
            f.array_except(
                f.array(
                    f.when(
                        f.col("pValueNegLog") < -np.log10(pvalue_cutoff),
                        "Subsignificant p-value",
                    ),
                    f.when(
                        f.col("position").isNull() & f.col("chromosome").isNull(),
                        "Incomplete genomic mapping",
                    ),
                    f.when(
                        f.col("chromosome").contains(" x "), "Composite association"
                    ),
                ),
                f.array(f.lit(None)),
            ),
        ).persist()
    )

    # Providing stats on the filtered association dataset:
    etl.logger.info(f"Number of associations: {association_df.count()}")
    etl.logger.info(
        f'Number of studies: {association_df.select("studyAccession").distinct().count()}'
    )
    etl.logger.info(
        f'Number of variants: {association_df.select("snpIds").distinct().count()}'
    )
    etl.logger.info(
        f'Number of failed associations: {association_df.filter(f.size(f.col("qualityControl")) != 0).count()}'
    )

    return association_df


def deconvolute_variants(associations: DataFrame) -> DataFrame:
    """Deconvoluting the list of variants attached to GWAS associations.

    We split the variant notation (chr, pos, snp id) into arrays, and flag associations where the
    number of genomic location is not consistent with the number of rsids

    Args:
        associations (DataFrame): DataFrame

    Returns:
        DataFrame: Flagged associations with inconsistent mappings.
    """
    return (
        associations
        # For further tracking, we save the variant of the association before splitting:
        .withColumn("gwasVariant", f.col("snpIds"))
        # Variant notation (chr, pos, snp id) are split into array:
        .withColumn("chromosome", f.split(f.col("chromosome"), ";"))
        .withColumn("position", f.split(f.col("position"), ";"))
        .withColumn(
            "strongestSnpRiskAllele",
            f.split(f.col("strongestSnpRiskAllele"), "; "),
        )
        .withColumn("snpIds", f.split(f.col("snpIds"), "; "))
        # Flagging associations where the genomic location is not consistent with rsids:
        .withColumn(
            "qualityControl",
            f.when(
                (f.size(f.col("qualityControl")) > 0)
                | (
                    (f.size(f.col("chromosome")) == f.size(f.col("position")))
                    & (
                        f.size(f.col("chromosome"))
                        == f.size(f.col("strongestSnpRiskAllele"))
                    )
                ),
                f.col("qualityControl"),
            ).otherwise(
                f.array_union(
                    f.col("qualityControl"), f.array(f.lit("Variant inconsistency"))
                )
            ),
        )
    )


def splitting_association(association: DataFrame) -> DataFrame:
    """Splitting associations based on the list of parseable varants from the GWAS Catalog.

    Args:
        association (DataFrame): DataFrame

    Returns:
        A dataframe with the same columns as the input dataframe, but with the variants exploded.
    """
    cols = association.columns
    variant_cols = [
        "chromosome",
        "position",
        "strongestSnpRiskAllele",
        "snpIds",
    ]
    return (
        association
        # Pairing together matching chr:pos:rsid in a list of structs:
        .withColumn(
            "variants",
            f.when(
                f.size(f.col("qualityControl")) == 0,
                f.arrays_zip(
                    f.col("chromosome"),
                    f.col("position"),
                    f.col("strongestSnpRiskAllele"),
                    f.col("snpIds"),
                ),
            ).otherwise(None),
        )
        # Exploding all variants:
        .select(
            *cols,
            f.explode_outer("variants").alias("variant"),
        )
        # Updating chr, pos, rsid, risk-snp-allele columns:
        .select(
            *[
                f.col(f"variant.{col}").alias(col) if col in variant_cols else col
                for col in cols
            ]
        )
    )


def process_associations(association: DataFrame) -> DataFrame:
    """Further cleaning association dataset.

    Steps:
    - Extracting risk allele if avaliable
    - Collecting all rsIds provided by GWAS Catalog to match with GnomAD
    - Parsing EFOs
    - Splitting p-values to mantissa and exponent.

    Args:
        association (DataFrame): Dataframe with associations

    Returns:
        Dataframe.
    """
    return association.select(
        "associationId",
        "studyAccession",
        parse_efos("mappedTraitUri").alias("AssociationEfos"),
        "associationDiseaseTrait",
        "chromosome",
        "position",
        "riskAlleleFrequency",
        "effectSize",
        "pValueAncestry",
        "confidenceInterval",
        "pValueText",
        "pValueNegLog",
        # Extracting p-value exponent and mantissa:
        f.split(f.col("pvalue"), "E")
        .getItem(1)
        .cast("integer")
        .alias("pValueExponent"),
        # Extracting risk allele:
        f.split(f.col("pvalue"), "E").getItem(0).cast("float").alias("pValueMantissa"),
        f.split(f.col("strongestSnpRiskAllele"), "-").getItem(1).alias("riskAllele"),
        "gwasVariant",
        # Collecting all available rsIDs provided by GWAS Catalog:
        f.array_distinct(
            f.array(
                f.split(f.col("strongestSnpRiskAllele"), "-").getItem(0),
                f.when(
                    f.col("snpIdCurrent").rlike("^[0-9]*$"),
                    f.format_string("rs%s", f.col("snpIdCurrent")),
                ).otherwise(f.col("snpIdCurrent")),
                f.col("snpIds"),
            )
        ).alias("rsIdsGwasCatalog"),
        "qualityControl",
    )


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
        f.col("variantId").isNull()
    ).count()
    etl.logger.info(
        f"Loading variant annotation and joining with associations... {assoc_without_variant} associations outside gnomAD"
    )
    return mapped_associations


def concordance_filter(df: DataFrame) -> DataFrame:
    """Concordance filter.

    This function filters out discordant GnomAD variant mappings. If the variant for an association could not be mapped, we still keep it.
    A risk allele is considered concordant if:
    - equal to the alt allele
    - equal to the ref allele
    - equal to the revese complement of the alt allele.
    - equal to the revese complement of the ref allele.
    - Or the risk allele is ambigious, noted by '?'

    Args:
        df (DataFrame): associations

    Returns:
        DataFrame: associations filtered for variants with concordant alleles with the risk allele.
    """
    return (
        df
        # Adding column with the reverse-complement of the risk allele:
        .withColumn(
            "riskAlleleReverseComplement",
            f.when(
                f.col("riskAllele").rlike(r"^[ACTG]+$"),
                f.reverse(f.translate(f.col("riskAllele"), "ACTG", "TGAC")),
            ).otherwise(f.col("riskAllele")),
        )
        # Adding columns flagging concordance:
        .withColumn(
            "isConcordant",
            # If risk allele is found on the positive strand:
            f.when(
                (f.col("riskAllele") == f.col("referenceAllele"))
                | (f.col("riskAllele") == f.col("alternateAllele")),
                True,
            )
            # If risk allele is found on the negative strand:
            .when(
                (f.col("riskAlleleReverseComplement") == f.col("referenceAllele"))
                | (f.col("riskAlleleReverseComplement") == f.col("alternateAllele")),
                True,
            )
            # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
            .when(f.col("riskAllele") == "?", True)
            # If the association could not be mapped we keep it:
            .when(f.col("variantId").isNull(), True)
            # Allele is discordant:
            .otherwise(False),
        )
        # Dropping discordant associations:
        .filter(f.col("isConcordant"))
        .drop("isConcordant", "riskAlleleReverseComplement")
        .persist()
    )


def filter_assoc_by_rsid(df: DataFrame) -> DataFrame:
    """Deduplication of GnomAD mappings based on rsIDs.

    A GnomAD mapping is kept if the rsID is consistent with the rsID coming from the GWAS Catalog OR
    we keep all mappings if no matching rsID is found for an association. We also keep all associations
    that are not mapped.

    Args:
        df (DataFrame): associations requiring:
            - associationId
            - rsIdsGwasCatalog
            - rsIdsGnomad

    Returns:
        DataFrame: filtered associations
    """
    w = Window.partitionBy("associationId")

    return (
        df
        # See if the GnomAD variant that was mapped to a given association has a matching rsId:
        .withColumn(
            "matchingRsId",
            f.when(
                f.size(
                    f.array_intersect(f.col("rsIdsGwasCatalog"), f.col("rsIdsGnomad"))
                )
                > 0,
                True,
            ).otherwise(False),
        )
        .withColumn(
            "successfulMappingExists",
            f.when(
                f.array_contains(f.collect_set(f.col("matchingRsId")).over(w), True),
                True,
            ).otherwise(False),
        )
        .filter(
            # Consistnet gnomad mapping exists:
            (f.col("matchingRsId") & f.col("successfulMappingExists"))
            # No consistent gnomad mapping:
            | (~f.col("matchingRsId") & ~f.col("successfulMappingExists"))
            # No mapping at all:
            | f.col("variantId").isNull()
        )
        .drop("successfulMappingExists", "matchingRsId")
        .persist()
    )


def filter_assoc_by_maf(associations: DataFrame) -> DataFrame:
    """Filter associations by Minor Allele Frequency.

    If an association is mapped to multiple concordant (or at least not discordant) variants,
    we only keep the one that has the highest minor allele frequency.

    This filter is based on the assumption that GWAS Studies are focusing on common variants mostly.
    This filter is especially designed to filter out rare alleles of multiallelics with matching rsIDs.

    Args:
        associations (DataFrame): associations

    Returns:
        DataFrame: associations filtered by allelic frequency
    """
    # Windowing over every association id, keeping only one mapping per association:
    w = Window.partitionBy("associationId").orderBy(f.desc("maf"))

    return (
        associations
        # Exploding all available population frequencies:
        .select("*", f.explode_outer(f.col("alleleFrequencies")).alias("af"))
        # Converting alt allele frequency to minor allele frequency:
        .withColumn(
            "maf",
            f.when(
                f.col("af.alleleFrequency") > 0.5, 1 - f.col("af.alleleFrequency")
            ).otherwise(f.col("af.alleleFrequency")),
        )
        .withColumn("row_number", f.row_number().over(w))
        # Selecting the variant with the highest minor-allele frequency:
        .filter(f.col("row_number") == 1)
        # Dropping helper columns:
        .drop("row_number", "af", "maf")
        .persist()
    )


def ingest_gwas_catalog_associations(
    etl: ETLSession,
    gwas_association_path: str,
    variant_annotation_path: str,
    pvalue_cutoff: float,
) -> DataFrame:
    """Ingest/process/map GWAS Catalog association.

    Args:
        etl (ETLSession): current ETL session
        gwas_association_path (str): GWAS catalogue dataset path
        variant_annotation_path (str): variant annotation dataset path
        pvalue_cutoff (float): GWAS significance threshold, flagging sub-significant associations

    Returns:
        DataFrame: Post-processed GWASCatalog associations
    """
    return (
        # Read associations:
        read_associations_data(etl, gwas_association_path, pvalue_cutoff)
        # Cleaning p-value text:
        .transform(lambda df: _pvalue_text_resolve(df, etl))
        # Parsing variants:
        .transform(deconvolute_variants)
        # Splitting associations by GWAS Catalog variants:
        .transform(splitting_association)
        # Minor formatting on the data:
        .transform(process_associations)
        # Mapping variants to GnomAD3:
        .transform(lambda df: map_variants(df, variant_annotation_path, etl))
        # Removing discordant mappings:
        .transform(concordance_filter)
        # Deduplicate associations by matching rsIDs:
        .transform(filter_assoc_by_rsid)
        # Deduplication by MAF:
        .transform(filter_assoc_by_maf)
        # Harmonizing association effect:
        .transform(harmonize_effect)
    )


def prepare_associations_for_pics(study_assoc: DataFrame) -> DataFrame:
    """Preparing the study/association table for PICS.

    It takes a dataframe of associations and returns a dataframe of associations with the gnomad sample
    information added

    Args:
        study_assoc (DataFrame): DataFrame

    Returns:
        A dataframe with columns for GnomAD sample proportion in respective population
    """
    association_columns = [
        "studyId",
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
        "variantId",
        "pValueMantissa",
        "pValueExponent",
        "beta",
        "beta_ci_lower",
        "beta_ci_upper",
        "odds_ratio",
        "odds_ratio_ci_lower",
        "odds_ratio_ci_upper",
        "qualityControl",
    ]

    # Selecting and de-duplicating columns:
    return (
        study_assoc.select(
            *association_columns,
            f.explode_outer("gnomadSamples").alias("gnomadSample"),
        )
        .select(
            *association_columns,
            f.col("gnomadSample.relativeSampleSize"),
            f.col("gnomadSample.gnomadPopulation"),
        )
        .distinct()
        .persist()
    )
