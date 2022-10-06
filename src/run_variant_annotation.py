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
        _load_refs=False,
    )

    # Generate struct for alt. allele frequency in selected populations:
    population_indices = ht.globals.freq_index_dict.collect()[0]
    population_indices = {pop: population_indices[f"{pop}-adj"] for pop in POPULATIONS}
    ht = ht.annotate(
        alleleFrequencies=hl.struct(
            **{pop: ht.freq[index].AF for pop, index in population_indices.items()}
        )
    )

    # Liftover
    grch37 = hl.get_reference("GRCh37")
    grch38 = hl.get_reference("GRCh38")
    grch38.add_liftover(chain_file, grch37)
    ht = ht.annotate(locus_GRCh37=hl.liftover(ht.locus, "GRCh37"))

    # Adding build-specific coordinates to the table:
    ht = ht.annotate(
        chromosome=ht.locus.contig.replace("chr", ""),
        position=ht.locus.position,
        chromosomeB37=ht.locus_GRCh37.contig.replace("chr", ""),
        positionB37=ht.locus_GRCh37.position,
        referenceAllele=ht.alleles[0],
        alternateAllele=ht.alleles[1],
        alleleType=ht.allele_info.allele_type,
        cadd=ht.cadd.rename({"raw_score": "raw"}).drop("has_duplicate"),
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
        ).rename({"rsid": "rsIds"}),
    )

    # Convert data:
    variants = (
        ht.select_globals()
        .to_spark(flatten=False)
        # Drop non biallelic variants
        .filter(f.size(f.col("alleles")) == 2)
        # Creating new column based on the transcript_consequences
        .select(
            "*",
            f.expr(
                "filter(vep.transcript_consequences, array -> array.canonical == True)"
            ).alias("transcript_consequences"),
            f.struct(
                f.col("vep.most_severe_consequence").alias("mostSevereConsequence"),
                f.col("vep.motif_feature_consequences").alias(
                    "motifFeatureConsequences"
                ),
                f.col("vep.regulatory_feature_consequences").alias(
                    "regulatoryFeatureConsequences"
                ),
                f.col("transcript_consequences").alias("transcriptConsequences"),
            ).alias("vep"),
            f.concat_ws(
                "_", "chromosome", "position", "referenceAllele", "alternateAllele"
            ).alias("id"),
            f.array(
                *[
                    f.struct(
                        f.col(f"alleleFrequencies.{pop}").alias("alleleFrequency"),
                        f.lit(pop).alias("populationName"),
                    )
                    for pop in POPULATIONS
                ]
            ).alias("alleleFrequencies"),
        )
        .drop("locus", "alleles", "transcript_consequences", "alleleFrequencies")
    )

    # validate_df_schema(variants, "variant_annotation.json")

    # Writing data partitioned by chromosome:
    (
        variants.write.mode(cfg.environment.sparkWriteMode)
        .partitionBy("chromosome")
        .parquet(output_file)
    )


if __name__ == "__main__":

    main()
