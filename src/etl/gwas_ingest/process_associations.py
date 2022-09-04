from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from etl.common.ETLSession import ETLSession

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


def filter_assoc_by_maf(df: DataFrame) -> DataFrame:
    """
    > We take the maximum minor allele frequency across all populations, and then drop all but the row
    with the highest minor allele frequency from all mappings. This process is population name agnostic,
    the only assumption is the field name is `allele_frequencies` and it is a structType.

    Args:
      df (DataFrame): DataFrame

    Returns:
      A DataFrame with identical columns
    """
    # parsing population names from schema:
    pop_names = [
        field for field in df.schema.fields if field.name == "allele_frequencies"
    ][0].dataType.fieldNames()

    def af2maf(c: Column) -> Column:
        """Column function to calculate minor allele frequency from allele frequency"""
        return f.when(c > 0.5, 1 - c).otherwise(c)

    # Windowing through all associations. Within an associations, rows are ordered by the maximum MAF:
    w = Window.partitionBy("association_id").orderBy(f.desc("maxMAF"))

    return (
        df.withColumn(
            "maxMAF",
            f.array_max(
                f.array(
                    *[af2maf(f.col(f"allele_frequencies.{pop}")) for pop in pop_names]
                )
            ),
        )
        .drop("allele_frequencies")
        .withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number")
        .persist()
    )


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

    return parsed_associations


def deduplicate(df: DataFrame) -> DataFrame:
    raise NotImplementedError


def filter_assoc_by_rsid(df: DataFrame) -> DataFrame:
    """
    > For each association, keeping all mappings with matching rsIDs,
    there is no mapping with matching rsId is found, keep all mappings.

    Args:
      df: The dataframe to filter with the following columns:
        - association_id
        - rsid_gwas_catalog
        - rsid_gnomad

    Returns:
      A dataframe with identical columns.
    """

    # Windowing through all associations:
    w = Window.partitionBy("association_id")

    return (
        df
        # See if the GnomAD variant that was mapped to a given association has a matching rsId:
        .withColumn(
            "matching_rsId",
            f.when(
                f.size(
                    f.array_intersect(f.col("rsid_gwas_catalog"), f.col("rsid_gnomad"))
                )
                > 0,
                True,
            ).otherwise(False),
        )
        .withColumn(
            "successful_mapping_exists",
            f.when(
                f.array_contains(f.collect_set(f.col("matching_rsId")).over(w), True),
                True,
            ).otherwise(False),
        )
        .filter(
            (f.col("matching_rsId") & f.col("successful_mapping_exists"))
            | (~f.col("matching_rsId") & ~f.col("successful_mapping_exists"))
        )
        .drop("successful_mapping_exists", "matching_rsId")
        .persist()
    )


def map_variants(
    parsed_associations: DataFrame, etl: ETLSession, varian_annotation: str
) -> DataFrame:
    """
    It reads the variant annotation from the `varian_annotation` path, and joins it with the parsed
    associations

    Args:
    parsed_associations (DataFrame): DataFrame
    etl (ETLSession): ETLSession
    varian_annotation (str): The path to the variant annotation file.

    Returns:
    A dataframe with the following columns:
        - chr_id
        - chr_pos
        - rsid_gnomad
        - ref
        - alt
        - variant_id
        - study_id
        - trait
        - pval
        - pval_text
        - beta
        - beta_text
        -
    """
    # Loading variant annotation and join with parsed associations:
    etl.logger.info("Loading variant annotation and joining with associations.")

    # Reading and joining variant annotation:
    variants = etl.spark.read.parquet(varian_annotation).select(
        f.col("chr").alias("chr_id"),
        f.col("pos_b38").alias("chr_pos"),
        f.col("rsid").alias("rsid_gnomad"),
        f.col("ref").alias("ref"),
        f.col("alt").alias("alt"),
        f.col("id").alias("variant_id"),
        f.col("af").alias("allele_frequencies"),
    )

    mapped_associations = variants.join(
        f.broadcast(parsed_associations), on=["chr_id", "chr_pos"], how="right"
    ).persist()

    return mapped_associations


def ingest_gwas_catalog_associations(
    etl: ETLSession,
    gwas_association_path: str,
    variant_annotation_path: str,
    pvalue_cutoff: float,
) -> DataFrame:

    """
    Main function to ingest/process/map GWAS Catalog association before the data should be joined with the studies.

    Args:
      etl (ETLSession): ETLSession
      gwas_association_path (str): GWAS catalogue dataset path
      variant_annotation_path (str): variant annotation dataset path
      pvalue_cutoff (float): GWAS significance threshold


    Returns:
      A DataFrame
    """

    gwas_associations = (
        # 1. Read associations:
        read_associations_data(etl, gwas_association_path, pvalue_cutoff)
        # 2. Process -> apply filter:
        .transform(lambda df: process_associations(df, etl))
        # 3. Map variants to GnomAD3:
        .transform(lambda df: map_variants(df, etl, variant_annotation_path))
        # 4. Remove discordants:
        .transform(concordance_filter)
        # 5. deduplicate associations by matching rsIDs:
        .transform(filter_assoc_by_rsid)
        # 6. deduplication by MAF:
        .transform(filter_assoc_by_maf)
    )

    return gwas_associations
