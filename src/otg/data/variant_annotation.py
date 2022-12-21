"""Variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import hail as hl
import pyspark.sql.functions as f
from omegaconf import MISSING

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import nullify_empty_array
from otg.common.utils import convert_gnomad_position_to_ensembl
from otg.data.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import ETLSession


@dataclass
class VariantAnnotationGnomadConfig:
    """Variant annotation from gnomad configuration."""

    path: str | None
    gnomad_file: str = MISSING
    chain_file: str = MISSING
    populations: list = MISSING


@dataclass
class VariantAnnotation(Dataset):
    """Creates a dataset with several annotations derived from GnomAD.

    Returns:
        DataFrame: Subset of variant annotations derived from GnomAD
    """

    schema: StructType = parse_spark_schema("variant_annotation.json")

    @classmethod
    def from_gnomad(
        cls: type[VariantAnnotation],
        etl: ETLSession,
        gnomad_file: str,
        chain_file: str,
        populations: list,
        path: str | None = None,
    ) -> VariantAnnotation:
        """Generate variant annotation dataset."""
        hl.init(sc=etl.spark.sparkContext, log="/tmp/hail.log")

        # Load variants dataset
        ht = hl.read_table(
            gnomad_file,
            _load_refs=False,
        )
        # Drop non biallelic variants
        ht = ht.filter(ht.alleles.length() == 2)

        # Generate struct for alt. allele frequency in selected populations:
        population_indices = ht.globals.freq_index_dict.collect()[0]
        population_indices = {
            pop: population_indices[f"{pop}-adj"] for pop in populations
        }
        ht = ht.annotate(
            alleleFrequenciesRaw=hl.struct(
                **{pop: ht.freq[index].AF for pop, index in population_indices.items()}
            )
        )

        # Liftover
        grch37 = hl.get_reference("GRCh37")
        grch38 = hl.get_reference("GRCh38")
        grch38.add_liftover(chain_file, grch37)
        ht = ht.annotate(locus_GRCh37=hl.liftover(ht.locus, "GRCh37"))

        # Adding build-specific coordinates to the table:
        ht = (
            # Creating a new column called gnomadVariantId which is a concatenation of chromosome, position,
            # referenceAllele and alternateAllele.
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

        df = (
            ht.select_globals()
            .to_spark(flatten=False)
            # Creating new column based on the transcript_consequences
            .withColumn(
                "gnomadVariantId",
                f.concat_ws(
                    "-",
                    "chromosome",
                    "position",
                    "referenceAllele",
                    "alternateAllele",
                ),
            )
            .withColumn(
                "ensembl_position",
                convert_gnomad_position_to_ensembl(
                    f.col("position"),
                    f.col("referenceAllele"),
                    f.col("alternateAllele"),
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
                            f.col(f"alleleFrequenciesRaw.{pop}").alias(
                                "alleleFrequency"
                            ),
                            f.lit(pop).alias("populationName"),
                        )
                        for pop in populations
                    ]
                ).alias("alleleFrequencies"),
                "cadd",
                f.struct(
                    f.col("vepRaw.most_severe_consequence").alias(
                        "mostSevereConsequence"
                    ),
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
        va = cls(df=df, path=path)
        va.validate_schema()
        return va

    def variant_index_parsing(self: VariantAnnotation) -> DataFrame:
        """It reads the variant annotation parquet file and formats it to follow the OTG variant model.

        Returns:
            DataFrame: A dataframe of variants and their annotation
        """
        unchanged_cols = [
            "id",
            "chromosome",
            "position",
            "referenceAllele",
            "alternateAllele",
            "chromosomeB37",
            "positionB37",
            "alleleType",
            "alleleFrequencies",
            "cadd",
        ]

        return self.df.select(
            *unchanged_cols,
            f.col("vep.mostSevereConsequence").alias("mostSevereConsequence"),
            # filters/rsid are arrays that can be empty, in this case we convert them to null
            nullify_empty_array(f.col("filters")).alias("filters"),
            nullify_empty_array(f.col("rsIds")).alias("rsIds"),
            f.lit(True).alias("variantInGnomad"),
        )
