"""Process GWAS catalog associations."""
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
    "DISEASE/TRAIT": "diseaseTrait",  # Reported trait of the study
    "MAPPED_TRAIT_URI": "mappedTraitUri",  # Mapped trait URIs of the study
    "MERGED": "merged",
    "P-VALUE (TEXT)": "pvalueText",  # Extra information on the association.
    # Association details:
    "P-VALUE": "pvalue",  # p-value of the association, string: split into exponent and mantissa.
    "PVALUE_MLOG": "pvalueMlog",  # -log10(p-value) of the association, float
    "OR or BETA": "effectSize",  # Effect size of the association, Odds ratio or beta
    "95% CI (TEXT)": "confidenceInterval",  # Confidence interval of the association, string: split into lower and upper bound.
    "CONTEXT": "context",
}


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
    # parsing population names from schema:
    for field in associations.schema.fields:
        if field.name == "alleleFrequencies" and isinstance(
            field.dataType, t.StructType
        ):
            pop_names = field.dataType.fieldNames()
            break

    def af2maf(c: Column) -> Column:
        """Column function to calculate minor allele frequency from allele frequency."""
        return f.when(c > 0.5, 1 - c).otherwise(c)

    # Windowing through all associations. Within an association, rows are ordered by the maximum MAF:
    w = Window.partitionBy("associationId").orderBy(f.desc("maxMAF"))

    return (
        associations.withColumn(
            "maxMAF",
            f.array_max(
                f.array(
                    *[af2maf(f.col(f"alleleFrequencies.{pop}")) for pop in pop_names]
                )
            ),
        )
        .withColumn("row_number", f.row_number().over(w))
        .filter(f.col("row_number") == 1)
        .drop("row_number", "alleleFrequencies")
        .persist()
    )


def concordance_filter(df: DataFrame) -> DataFrame:
    """Concordance filter.

    This function filters for variants with concordant alleles with the association's reported risk allele.
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
                (f.col("riskAllele") == f.col("ref"))
                | (f.col("riskAllele") == f.col("alt")),
                True,
            )
            # If risk allele is found on the negative strand:
            .when(
                (f.col("riskAlleleReverseComplement") == f.col("ref"))
                | (f.col("riskAlleleReverseComplement") == f.col("alt")),
                True,
            )
            # If risk allele is ambiguous, still accepted: < This condition could be reconsidered
            .when(f.col("riskAllele") == "?", True)
            # Allele is discordant:
            .otherwise(False),
        )
        # Dropping discordant associations:
        .filter(f.col("isConcordant"))
        .drop("isConcordant", "riskAlleleReverseComplement")
        .persist()
    )


def read_associations_data(
    etl: ETLSession, gwas_association_file: str, pvalue_cutoff: float
) -> DataFrame:
    """Read GWASCatalog associations.

    It reads the GWAS Catalog association dataset, selects and renames columns, casts columns, and
    applies some pre-defined filters on the data

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
            ]
        )
        # Cast minus log p-value as float:
        .withColumn("pvalueMlog", f.col("pvalueMlog").cast(t.FloatType()))
        # Apply some pre-defined filters on the data:
        # 1. Dropping associations based on variant x variant interactions
        # 2. Dropping sub-significant associations
        # 3. Dropping associations without genomic location
        .filter(
            ~f.col("chrId").contains(" x ")
            & (f.col("pvalueMlog") >= -np.log10(pvalue_cutoff))
            & (f.col("chrPos").isNotNull() & f.col("chrId").isNotNull())
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

    return association_df


def process_associations(association_df: DataFrame, etl: ETLSession) -> DataFrame:
    """Post-process associations DataFrame.

    - The function does the following:
        - Adds a unique identifier to each association.
        - Processes the variant related columns.
        - Processes the EFO terms.
        - Splits the p-value into exponent and mantissa.
        - Drops some columns.
        - Provides some stats on the filtered association dataset.

    Args:
        association_df (DataFrame): associations
        etl (ETLSession): current ETL session

    Returns:
        DataFrame: associations including the next columns:
            - associationId
            - snpIdCurrent
            - chrId
            - chrPos
            - snpIds
            - riskAllele
            - rsidGwasCatalog
            - efo
            - exponent
            - mantissa
    """
    # Processing associations:
    parsed_associations = (
        # spark.read.csv(associations, sep='\t', header=True)
        association_df
        # Adding association identifier for future deduplication:
        .withColumn("associationId", f.monotonically_increasing_id())
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
            "snpIdCurrent",
            f.when(
                f.col("snpIdCurrent").rlike("^[0-9]*$"),
                f.format_string("rs%s", f.col("snpIdCurrent")),
            ).otherwise(f.col("snpIdCurrent")),
        )
        # Variant notation (chr, pos, snp id) are split into array:
        .withColumn("chrId", f.split(f.col("chrId"), ";"))
        .withColumn("chrPos", f.split(f.col("chrPos"), ";"))
        .withColumn(
            "strongestSnpRiskAllele",
            f.split(f.col("strongestSnpRiskAllele"), "; "),
        )
        .withColumn("snpIds", f.split(f.col("snpIds"), "; "))
        # Variant fields are joined together in a matching list, then extracted into a separate rows again:
        .withColumn(
            "VARIANT",
            f.explode(
                f.arrays_zip("chrId", "chrPos", "strongestSnpRiskAllele", "snpIds")
            ),
        )
        # Updating variant columns:
        .withColumn("snpIds", f.col("VARIANT.snpIds"))
        .withColumn("chrId", f.col("VARIANT.chrId"))
        .withColumn("chrPos", f.col("VARIANT.chrPos").cast(t.IntegerType()))
        .withColumn("strongestSnpRiskAllele", f.col("VARIANT.strongestSnpRiskAllele"))
        # Extracting risk allele:
        .withColumn(
            "riskAllele", f.split(f.col("strongestSnpRiskAllele"), "-").getItem(1)
        )
        # Create a unique set of SNPs linked to the assocition:
        .withColumn(
            "rsIdsGwasCatalog",
            f.array_distinct(
                f.array(
                    f.split(f.col("strongestSnpRiskAllele"), "-").getItem(0),
                    f.col("snpIdCurrent"),
                    f.col("snpIds"),
                )
            ),
        )
        # Processing EFO terms:
        #   - Multiple EFO terms can correspond to a single association.
        #   - EFO terms are stored as full URIS, separated by semicolons.
        #   - Associations are exploded to all EFO terms.
        #   - EFO terms in the study table is not considered as association level EFO annotation has priority (via p-value text)
        # Process EFO URIs: -> why do we explode?
        # .withColumn('efo', F.explode(F.expr(r"regexp_extract_all(mappedTraitUri, '([A-Z]+_[0-9]+)')")))
        .withColumn(
            "efo", f.expr(r"regexp_extract_all(mappedTraitUri, '([A-Z]+_[0-9]+)')")
        )
        # Splitting p-value into exponent and mantissa:
        .withColumn(
            "exponent", f.split(f.col("pvalue"), "E").getItem(1).cast("integer")
        )
        .withColumn("mantissa", f.split(f.col("pvalue"), "E").getItem(0).cast("float"))
        # Cleaning up:
        .drop("mappedTraitUri", "strongestSnpRiskAllele", "VARIANT")
        .persist()
    )

    # Providing stats on the filtered association dataset:
    etl.logger.info(f"Number of associations: {parsed_associations.count()}")
    etl.logger.info(
        f'Number of studies: {parsed_associations.select("studyAccession").distinct().count()}'
    )
    etl.logger.info(
        f'Number of variants: {parsed_associations.select("snpIds").distinct().count()}'
    )

    return parsed_associations


def deduplicate(df: DataFrame) -> DataFrame:
    """Deduplicate DataFrame (not implemented)."""
    raise NotImplementedError


def filter_assoc_by_rsid(df: DataFrame) -> DataFrame:
    """Filter associations by rsid.

    Args:
        df (DataFrame): associations requiring:
            - associationId
            - rsIdsGwasCatalog
            - rsidGnomad

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
                    f.array_intersect(f.col("rsIdsGwasCatalog"), f.col("rsidGnomad"))
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
            (f.col("matchingRsId") & f.col("successfulMappingExists"))
            | (~f.col("matchingRsId") & ~f.col("successfulMappingExists"))
        )
        .drop("successfulMappingExists", "matchingRsId")
        .persist()
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
        "chromosome",
        "position",
        "rsIdsGnomad",
        "referenceAllele",
        "alternateAllele",
        "alleleFrequencies",
    )

    mapped_associations = variants.join(
        f.broadcast(parsed_associations), on=["chromosome", "position"], how="right"
    ).persist()
    assoc_without_variant = mapped_associations.filter(
        f.col("variantId").isNull()
    ).count()
    etl.logger.info(
        f"Loading variant annotation and joining with associations... {assoc_without_variant} associations outside gnomAD"
    )
    return mapped_associations


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
        pvalue_cutoff (float): GWAS significance threshold

    Returns:
        DataFrame: Post-processed GWASCatalog associations
    """
    return (
        # 1. Read associations:
        read_associations_data(etl, gwas_association_path, pvalue_cutoff)
        # 2. Process -> apply filter:
        .transform(lambda df: process_associations(df, etl))
        # 3. Map variants to GnomAD3:
        .transform(lambda df: map_variants(df, variant_annotation_path, etl))
        # 4. Remove discordants:
        .transform(concordance_filter)
        # 5. deduplicate associations by matching rsIDs:
        .transform(filter_assoc_by_rsid)
        # 6. deduplication by MAF:
        .transform(filter_assoc_by_maf)
    )
