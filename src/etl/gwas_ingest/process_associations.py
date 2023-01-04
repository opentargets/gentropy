"""Process GWAS catalog associations."""
from __future__ import annotations

import importlib.resources as pkg_resources
import json
from typing import TYPE_CHECKING

import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import types as t

from etl.gwas_ingest.association_mapping import clean_mappings, map_variants
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
    flags associations that do not reach GWAS significance level.

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
        )
        # Parsing association EFO:
        .withColumn("associationEfos", parse_efos("mappedTraitUri")).persist()
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


def _collect_rsids(
    snp_id: Column, snp_id_current: Column, risk_allele: Column
) -> Column:
    """It takes three columns, and returns an array of distinct values from those columns.

    Args:
        snp_id (Column): The original snp id from the GWAS catalog.
        snp_id_current (Column): The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good. The
        risk_allele (Column): The risk allele for the SNP.

    Returns:
        An array of distinct values.
    """
    # The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
    snp_id_current = f.when(
        snp_id_current.rlike("^[0-9]*$"),
        f.format_string("rs%s", snp_id_current),
    )
    # Cleaning risk allele:
    risk_allele = f.split(risk_allele, "-").getItem(0)

    # Collecting all values:
    return f.array_distinct(f.array(snp_id, snp_id_current, risk_allele))


def splitting_association(association: DataFrame) -> DataFrame:
    """Splitting associations based on the list of parseable variants from the GWAS Catalog.

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
                ~f.array_contains(f.col("qualityControl"), "Variant inconsistency"),
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
            ],
            # Extract p-value exponent:
            f.split(f.col("pvalue"), "E")
            .getItem(1)
            .cast("integer")
            .alias("pValueExponent"),
            # Extract p-value mantissa:
            f.split(f.col("pvalue"), "E")
            .getItem(0)
            .cast("float")
            .alias("pValueMantissa"),
        )
        # Extracting risk allele:
        .withColumn(
            "riskAllele", f.split(f.col("strongestSnpRiskAllele"), "-").getItem(1)
        )
        # Creating list of rsIds from gwas catalog dataset:
        .withColumn(
            "rsIdsGwasCatalog",
            _collect_rsids(
                f.col("snpIds"), f.col("snpIdCurrent"), f.col("strongestSnpRiskAllele")
            ),
        )
        # Dropping some un-used column:
        .drop(
            "pValue",
            "riskAlleleFrequency",
            "cnv",
            "strongestSnpRiskAllele",
            "snpIdCurrent",
            "snpIds",
            "pValue",
            "mappedTraitUri",
            "pValueNegLog",
        )
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
        # Mapping variants to GnomAD3:
        .transform(lambda df: map_variants(df, variant_annotation_path, etl))
        # Cleaning GnomAD variant mappings to ensure no duplication happens:
        .transform(clean_mappings)
        # Harmonizing association effect:
        .transform(harmonize_effect)
    )


def generate_association_table(df: DataFrame) -> DataFrame:
    """Generating top-loci table.

    Args:
        df (DataFrame): DataFrame

    Returns:
        A DataFrame with the columns specified in the filter_columns list.
    """
    filter_columns = [
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
        "variantId",
        "studyId",
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
    return (
        df.select(*filter_columns)
        .filter(f.col("variantId").isNotNull())
        .distinct()
        .persist()
    )
