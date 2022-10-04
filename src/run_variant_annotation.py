"""Step to generate variant annotation dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

import hydra

if TYPE_CHECKING:
    from omegaconf import DictConfig

import hail as hl
import pyspark.sql.functions as f

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


@hydra.main(version_base=None, config_path=".", config_name="config")
def main(cfg: DictConfig) -> None:
    """Run variant annotation generation."""
    # establish spark connection
    etl = ETLSession(cfg)
    hl.init(sc=etl.spark.sparkContext)

    # Extracting parameters:
    chain_file = cfg.etl.variant_annotation.inputs.chain_file
    output_file = cfg.etl.variant_annotation.outputs.variant_annotation

    # Load data
    ht = hl.read_table(
        cfg.etl.variant_annotation.inputs.gnomad_file,
        _n_partitions=cfg.etl.variant_annotation.parameters.partition_count,
        _load_refs=False,
    )

    # Assert that all alleles are biallelic:
    assert ht.all(
        ht.alleles.length() == 2
    ), "Mono- or multiallelic variants have been found."

    # Extracting AF indices of populations:
    population_indices = ht.globals.freq_index_dict.collect()[0]
    population_indices = {pop: population_indices[f"{pop}-adj"] for pop in POPULATIONS}

    # Generate struct for alt. allele frequency in selected populations:
    ht = ht.annotate(
        af=hl.struct(
            **{pop: ht.freq[index].AF for pop, index in population_indices.items()}
        )
    )
    # Add chain file
    grch37 = hl.get_reference("GRCh37")
    grch38 = hl.get_reference("GRCh38")
    grch38.add_liftover(chain_file, grch37)

    # Liftover
    ht = ht.annotate(locus_GRCh37=hl.liftover(ht.locus, "GRCh37"))

    # Adding build-specific coordinates to the table:
    ht = ht.annotate(
        chrom=ht.locus.contig.replace("chr", ""),
        pos=ht.locus.position,
        chrom_b37=ht.locus_GRCh37.contig.replace("chr", ""),
        pos_b37=ht.locus_GRCh37.position,
        ref=ht.alleles[0],
        alt=ht.alleles[1],
        allele_type=ht.allele_info.allele_type,
    )

    # Updating table:
    ht = ht.annotate(
        # Updating CADD column:
        cadd=ht.cadd.rename({"raw_score": "raw"}).drop("has_duplicate"),
    )

    # Drop all global annotations:
    ht = ht.select_globals()

    # Drop unnecessary VEP fields
    ht = ht.annotate(
        vep=ht.vep.drop(
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
        )
    )

    # Convert data:
    variants = (
        # Select columns and convert to pyspark:
        ht.rename(
            {
                "chrom": "chromosome",
                "pos": "position",
                "chrom_b37": "chromosomeB37",
                "pos_b37": "positionB37",
                "ref": "referenceAllele",
                "alt": "alternateAllele",
                "allele_type": "alleleType",
                "rsid": "rsIds",
                "af": "alleleFrequencies",
            }
        )
        .select(
            "chromosome",
            "position",
            "chromosomeB37",
            "positionB37",
            "referenceAllele",
            "alternateAllele",
            "alleleType",
            "vep",
            "rsIds",
            "alleleFrequencies",
            "cadd",
            "filters",
        )
        .to_spark(flatten=False)
        .repartition(cfg.etl.variant_annotation.parameters.partition_count)
        # Creating new column based on the transcript_consequences
        .select(
            "*",
            f.expr(
                "filter(vep.transcript_consequences, array -> array.canonical == True)"
            ).alias("transcript_consequences"),
        )
        # Re-creating the vep column with the new transcript consequence object:
        .withColumn(
            "vep",
            f.struct(
                f.col("vep.most_severe_consequence").alias("mostSevereConsequence"),
                f.col("vep.motif_feature_consequences").alias(
                    "motifFeatureConsequences"
                ),
                f.col("vep.regulatory_feature_consequences").alias(
                    "regulatoryFeatureConsequences"
                ),
                f.col("transcript_consequences").alias("transcriptConsequences"),
            ),
        )
        # Generate variant id column:
        .withColumn(
            "id",
            f.concat_ws(
                "_", "chromosome", "position", "referenceAllele", "alternateAllele"
            ),
        )
        # Create new allele frequency column:
        .withColumn(
            "alleleFrequencies",
            f.array(
                *[
                    f.struct(
                        f.col(f"alleleFrequencies.{pop}").alias("alleleFrequency"),
                        f.lit(pop).alias("populationName"),
                    )
                    for pop in POPULATIONS
                ]
            ),
        )
        # Drop unused column:
        .drop("locus", "alleles", "transcript_consequences", "alleleFrequencies")
    )

    # validate_df_schema(variants, "variant_annotation.json")

    # Writing data partitioned by chromosome:
    (
        variants.partitionBy("chromosome")
        .write.mode(cfg.environment.sparkWriteMode)
        .parquet(output_file)
    )


if __name__ == "__main__":

    main()
