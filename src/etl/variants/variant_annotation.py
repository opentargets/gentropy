"""Step to generate variant annotation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hail as hl
import pyspark.sql.functions as f

from etl.common.utils import convert_gnomad_position_to_ensembl

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from etl.common.ETLSession import ETLSession


# Population of interest:
POPULATIONS = {
    "afr",  # African-American
    "amr",  # American Admixed/Latino
    "ami",  # Amish ancestry
    "asj",  # Ashkenazi Jewish
    "eas",  # East Asian
    "fin",  # Finnish
    "nfe",  # Non-Finnish European
    "mid",  # Middle Eastern
    "sas",  # South Asian
    "oth",  # Other
}


def generate_variant_annotation(
    etl: ETLSession, gnomad_variants_path: str, chain_file_path: str
) -> DataFrame:
    """Creates a dataset with several annotations derived from GnomAD.

    Args:
        etl (ETLSession): ETL session
        gnomad_variants_path (str): Path to the GnomAD variants dataset
        chain_file_path (str): Chain to liftover from grch38 to grch37

    Returns:
        DataFrame: Subset of variant annotations derived from GnomAD
    """
    etl.logger.info("Generating variant annotation...")

    hl.init(sc=etl.spark.sparkContext)

    # Load variants dataset
    ht = hl.read_table(
        gnomad_variants_path,
        _load_refs=False,
    )
    # Drop non biallelic variants
    ht = ht.filter(ht.alleles.length() == 2)

    # Generate struct for alt. allele frequency in selected populations:
    population_indices = ht.globals.freq_index_dict.collect()[0]
    population_indices = {pop: population_indices[f"{pop}-adj"] for pop in POPULATIONS}
    ht = ht.annotate(
        alleleFrequenciesRaw=hl.struct(
            **{pop: ht.freq[index].AF for pop, index in population_indices.items()}
        )
    )

    # Liftover
    grch37 = hl.get_reference("GRCh37")
    grch38 = hl.get_reference("GRCh38")
    grch38.add_liftover(chain_file_path, grch37)
    ht = ht.annotate(locus_GRCh37=hl.liftover(ht.locus, "GRCh37"))

    # Adding build-specific coordinates to the table:
    ht = (
        ht.annotate(
            chromosome=ht.locus.contig.replace("chr", ""),
            position=ht.locus.position,
            chromosomeB37=ht.locus_GRCh37.contig.replace("chr", ""),
            positionB37=ht.locus_GRCh37.position,
            referenceAllele=ht.alleles[0],
            alternateAllele=ht.alleles[1],
            alleleType=ht.allele_info.allele_type,
            cadd=ht.cadd.rename({"raw_score": "raw"}).drop("has_duplicate"),
            vepRaw=ht.vep.drop(
                "assembly_name",
                "allele_string",
                "ancestral",
                "context",
                "end",
                "id",
                "input",
                "intergenic_consequences",
                "seq_region_name",
                "start",
                "strand",
                "variant_class",
            ),
        )
        .rename({"rsid": "rsIds"})
        .drop("vep")
    )

    return (
        ht.select_globals()
        .to_spark(flatten=False)
        # Creating new column based on the transcript_consequences
        .withColumn(
            "gnomadVariantId",
            f.concat_ws(
                "-", "chromosome", "position", "referenceAllele", "alternateAllele"
            ),
        )
        .withColumn(
            "ensembl_position",
            convert_gnomad_position_to_ensembl(
                f.col("position"), f.col("referenceAllele"), f.col("alternateAllele")
            ),
        )
        .select(
            f.concat_ws(
                "_",
                "chromosome",
                "ensembl_position",
                "referenceAllele",
                "alternateAllele",
            ).alias("id"),
            "chromosome",
            f.col("ensembl_position").alias("position"),
            "referenceAllele",
            "alternateAllele",
            "chromosomeB37",
            "positionB37",
            "gnomadVariantId",
            "alleleType",
            "rsIds",
            f.array(
                *[
                    f.struct(
                        f.col(f"alleleFrequenciesRaw.{pop}").alias("alleleFrequency"),
                        f.lit(pop).alias("populationName"),
                    )
                    for pop in POPULATIONS
                ]
            ).alias("alleleFrequencies"),
            "cadd",
            f.struct(
                f.col("vepRaw.most_severe_consequence").alias("mostSevereConsequence"),
                f.col("vepRaw.motif_feature_consequences").alias(
                    "motifFeatureConsequences"
                ),
                f.col("vepRaw.regulatory_feature_consequences").alias(
                    "regulatoryFeatureConsequences"
                ),
                # Non canonical transcripts and gene IDs other than ensembl are filtered out
                f.expr(
                    "filter(vepRaw.transcript_consequences, array -> (array.canonical == 1) and (array.gene_symbol_source == 'HGNC'))"
                ).alias("transcriptConsequences"),
            ).alias("vep"),
            "filters",
        )
    )
