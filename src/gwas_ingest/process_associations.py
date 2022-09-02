from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import types as t

if TYPE_CHECKING:
    from omegaconf import DictConfig
    from pyspark.sql import DataFrame

    from common.ETLSession import ETLSession

ASSOCIATION_COLUMNS_MAP = {
    # Variant related columns:
    "STRONGEST SNP-RISK ALLELE": "strongest_snp_risk_allele",  # variant id and the allele is extracted (; separated list)
    "CHR_ID": "chr_id",  # Mapped genomic location of the variant (; separated list)
    "CHR_POS": "chr_pos",
    "RISK ALLELE FREQUENCY": "risk_allele_frequency",
    "CNV": "cnv",  # Flag if a variant is a copy number variant
    "SNP_ID_CURRENT": "snp_id_current",  #
    "SNPS": "snp_ids",  # List of all SNPs associated with the variant
    # Study related columns:
    "STUDY ACCESSION": "study_accession",
    # Disease/Trait related columns:
    "DISEASE/TRAIT": "disease_trait",  # Reported trait of the study
    "MAPPED_TRAIT_URI": "mapped_trait_uri",  # Mapped trait URIs of the study
    "MERGED": "merged",
    "P-VALUE (TEXT)": "p_value_text",  # Extra information on the association.
    # Association details:
    "P-VALUE": "p_value",  # p-value of the association, string: split into exponent and mantissa.
    "PVALUE_MLOG": "pvalue_mlog",  # -log10(p-value) of the association, float
    "OR or BETA": "or_beta",  # Effect size of the association, Odds ratio or beta
    "95% CI (TEXT)": "confidence_interval",  # Confidence interval of the association, string: split into lower and upper bound.
    "CONTEXT": "context",
}


def concordance_filter(df: DataFrame) -> DataFrame:
    """
    This function filters for variants with concordant alleles witht the association's reported risk allele.
    A risk allele is considered concordant if:
    - equal to the alt allele
    - equal to the ref allele
    - equal to the revese complement of the alt allele.
    - equal to the revese complement of the ref allele.
    - Or the risk allele is ambigious, noted by '?'

    Args:
      df (DataFrame): DataFrame

    Returns:
      A dataframe with identical columns, except with filtered rows for variants
      with concordant alleles with the risk allele of the association.
    """

    logging.info("Dropping discordant mappings...")
    return (
        df
        # Adding column with the reverse-complement of the risk allele:
        .withColumn(
            "risk_allele_reverse_complement",
            f.when(
                f.col("risk_allele").rlike(r"^[ACTG]+$"),
                f.reverse(f.translate(f.col("risk_allele"), "ACTG", "TGAC")),
            ).otherwise(f.col("risk_allele")),
        )
        # Adding columns flagging concordance:
        .withColumn(
            "is_concordant",
            # If risk allele is found on the positive strand:
            f.when(
                (f.col("risk_allele") == f.col("ref"))
                | (f.col("risk_allele") == f.col("alt")),
                True,
            )
            # If risk allele is found on the negative strand:
            .when(
                (f.col("risk_allele_reverse_complement") == f.col("ref"))
                | (f.col("risk_allele_reverse_complement") == f.col("alt")),
                True,
            )
            # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
            .when(f.col("risk_allele") == "?", True)
            # Allele is discordant:
            .otherwise(False),
        )
        # Dropping discordant associations:
        .filter(f.col("is_concordant"))
        .drop("is_concordant", "risk_allele_reverse_complement")
        .persist()
    )


def read_associations_data(
    etl: ETLSession, gwas_association_file: str, pvalue_cutoff: float
) -> DataFrame:
    """
    It reads the GWAS Catalog association dataset, selects and renames columns, casts columns, and
    applies some pre-defined filters on the data

    The function returns a `DataFrame` with the GWAS Catalog associations

    Args:
      etl (ETLSession): ETLSession
      gwas_association_file (str): The path to the GWAS Catalog associations file.
      pvalue_cutoff (float): The p-value threshold for filtering associations.

    Returns:
      A dataframe with the GWAS Catalog associations.
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
            ]
        )
        # Cast minus log p-value as float:
        .withColumn("pvalue_mlog", f.col("pvalue_mlog").cast(t.FloatType()))
        # Apply some pre-defined filters on the data:
        # 1. Dropping associations based on variant x variant interactions
        # 2. Dropping sub-significant associations
        # 3. Dropping associations without genomic location
        .filter(
            ~f.col("chr_id").contains(" x ")
            & (f.col("pvalue_mlog") >= -np.log10(pvalue_cutoff))
            & (f.col("chr_pos").isNotNull() & f.col("chr_id").isNotNull())
        ).persist()
    )

    # Providing stats on the filtered association dataset:
    etl.logger.info(f"Number of associations: {association_df.count()}")
    etl.logger.info(
        f'Number of studies: {association_df.select("study_accession").distinct().count()}'
    )
    etl.logger.info(
        f'Number of variants: {association_df.select("snp_ids").distinct().count()}'
    )
    etl.logger.info(f"Assocation schema: {association_df.show(1, False, True)}")

    return association_df


def process_associations(association_df: DataFrame, etl: ETLSession) -> DataFrame:
    """
    - The function takes a dataframe as input, and returns a dataframe as output.
    - The output dataframe is the parsed GWAS catalog associations file.
    - The function does the following:
        - Adds a unique identifier to each association.
        - Processes the variant related columns.
        - Processes the EFO terms.
        - Splits the p-value into exponent and mantissa.
        - Drops some columns.
        - Provides some stats on the filtered association dataset.

    Args:
      association_df (DataFrame): DataFrame

    Returns:
      A dataframe with the following columns:
        - association_id
        - snp_id_current
        - chr_id
        - chr_pos
        - snp_ids
        - risk_allele
        - rsid_gwas_catalog
        - efo
        - exponent
        - mantissa
    """

    # Processing associations:
    parsed_associations = (
        # spark.read.csv(associations, sep='\t', header=True)
        association_df
        # Adding association identifier for future deduplication:
        .withColumn("association_id", f.monotonically_increasing_id())
        # Processing variant related columns:
        #   - Sorting out current rsID field: <- why do we need this? rs identifiers should always come from the GnomAD dataset.
        #   - Removing variants with no genomic mappings -> losing ~3% of all associations
        #   - Multiple variants can correspond to a single association.
        #   - Variant identifiers are stored in the SNPS column, while the mapped coordinates are stored in the CHR_ID and CHR_POS columns.
        #   - All these fields are split into arrays, then they are paired with the same index eg. first ID is paired with first coordinate, and so on
        #   - Then the association is exploded to all variants.
        #   - The risk allele is extracted from the 'STRONGEST SNP-RISK ALLELE' column.
        # The current snp id field is just a number at the moment (stored as a string). Adding 'rs' prefix if looks good.
        .withColumn(
            "snp_id_current",
            f.when(
                f.col("snp_id_current").rlike("^[0-9]*$"),
                f.format_string("rs%s", f.col("snp_id_current")),
            ).otherwise(f.col("snp_id_current")),
        )
        # Variant notation (chr, pos, snp id) are split into array:
        .withColumn("chr_id", f.split(f.col("chr_id"), ";"))
        .withColumn("chr_pos", f.split(f.col("chr_pos"), ";"))
        .withColumn(
            "strongest_snp_risk_allele",
            f.split(f.col("strongest_snp_risk_allele"), "; "),
        )
        .withColumn("snp_ids", f.split(f.col("snp_ids"), "; "))
        # Variant fields are joined together in a matching list, then extracted into a separate rows again:
        .withColumn(
            "VARIANT",
            f.explode(
                f.arrays_zip(
                    "chr_id", "chr_pos", "strongest_snp_risk_allele", "snp_ids"
                )
            ),
        )
        # Updating variant columns:
        .withColumn("snp_ids", f.col("VARIANT.snp_ids"))
        .withColumn("chr_id", f.col("VARIANT.chr_id"))
        .withColumn("chr_pos", f.col("VARIANT.chr_pos").cast(t.IntegerType()))
        .withColumn(
            "strongest_snp_risk_allele", f.col("VARIANT.strongest_snp_risk_allele")
        )
        # Extracting risk allele:
        .withColumn(
            "risk_allele", f.split(f.col("strongest_snp_risk_allele"), "-").getItem(1)
        )
        # Create a unique set of SNPs linked to the assocition:
        .withColumn(
            "rsid_gwas_catalog",
            f.array_distinct(
                f.array(
                    f.split(f.col("strongest_snp_risk_allele"), "-").getItem(0),
                    f.col("snp_id_current"),
                    f.col("snp_ids"),
                )
            ),
        )
        # Processing EFO terms:
        #   - Multiple EFO terms can correspond to a single association.
        #   - EFO terms are stored as full URIS, separated by semicolons.
        #   - Associations are exploded to all EFO terms.
        #   - EFO terms in the study table is not considered as association level EFO annotation has priority (via p-value text)
        # Process EFO URIs: -> why do we explode?
        # .withColumn('efo', F.explode(F.expr(r"regexp_extract_all(mapped_trait_uri, '([A-Z]+_[0-9]+)')")))
        .withColumn(
            "efo", f.expr(r"regexp_extract_all(mapped_trait_uri, '([A-Z]+_[0-9]+)')")
        )
        # Splitting p-value into exponent and mantissa:
        .withColumn(
            "exponent", f.split(f.col("p_value"), "E").getItem(1).cast("integer")
        )
        .withColumn("mantissa", f.split(f.col("p_value"), "E").getItem(0).cast("float"))
        # Cleaning up:
        .drop("mapped_trait_uri", "strongest_snp_risk_allele", "VARIANT")
        .persist()
    )

    # Providing stats on the filtered association dataset:
    etl.logger.info(f"Number of associations: {parsed_associations.count()}")
    etl.logger.info(
        f'Number of studies: {parsed_associations.select("study_accession").distinct().count()}'
    )
    etl.logger.info(
        f'Number of variants: {parsed_associations.select("snp_ids").distinct().count()}'
    )
    etl.logger.info(f"Assocation: {parsed_associations.show(2, False, True)}")

    return parsed_associations


def deduplicate(df: DataFrame) -> DataFrame:
    raise NotImplementedError


def map_variants(df: DataFrame) -> DataFrame:
    raise NotImplementedError


def ingest_gwas_catalog_associations(etl: ETLSession, cfg: DictConfig) -> DataFrame:

    gwas_association_file = cfg.gwas_ingest.input.gwas_catalog_associations
    pvalue_cutoff = cfg.gwas_ingest.parameters.p_value_cutoff
    logger = etl.logger

    gwas_associations = (
        # 1. Read associations.
        read_associations_data(etl, gwas_association_file, pvalue_cutoff)
        # 2. Process -> apply filter
        .transform(process_associations, args=[logger])
        # 3. Map
        # .transform(map_variants)
        # # 4. Remove discordants:
        # .transform(concordance_filter)
        # # 5. deduplicate associations:
        # .transform(deduplicate).persist()
    )

    return gwas_associations
