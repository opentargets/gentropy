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
        chrom_b38=ht.locus.contig.replace("chr", ""),
        pos_b38=ht.locus.position,
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
        # Adding locus as new column:
        locus_GRCh38=ht.locus,
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

    # Sort columns
    col_order = [
        "locus_GRCh38",
        "chrom_b38",
        "pos_b38",
        "chrom_b37",
        "pos_b37",
        "ref",
        "alt",
        "allele_type",
        "vep",
        "rsid",
        "af",
        "cadd",
        "filters",
    ]

    # Convert data:
    (
        # Select columns and convert to pyspark:
        ht.select(*col_order)
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
                f.col("vep.most_severe_consequence").alias("most_severe_consequence"),
                f.col("vep.motif_feature_consequences").alias(
                    "motif_feature_consequences"
                ),
                f.col("vep.regulatory_feature_consequences").alias(
                    "regulatory_feature_consequences"
                ),
                f.col("transcript_consequences").alias("transcript_consequences"),
            ),
        )
        # Generate variant id column:
        .withColumn("id", f.concat_ws("_", "chrom_b38", "pos_b38", "ref", "alt"))
        # Create new allele frequency column:
        .withColumn(
            "alleleFrequencies",
            f.array(
                *[
                    f.struct(
                        f.col(f"af.{pop}").alias("alleleFrequency"),
                        f.lit(pop).alias("populationName"),
                    )
                    for pop in POPULATIONS
                ]
            ),
        )
        # Drop unused column:
        .drop("transcript_consequences", "af")
        # Adding new column:
        .withColumn("chr", f.col("chrom_b38"))
        # Writing data partitioned by chromosome:
        .write.mode(cfg.environment.sparkWriteMode)
        .partitionBy("chr")
        .parquet(output_file)
    )


if __name__ == "__main__":

    main()
