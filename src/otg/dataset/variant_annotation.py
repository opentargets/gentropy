"""Variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import hail as hl
import pyspark.sql.functions as f

from otg.common.schemas import parse_spark_schema
from otg.common.spark_helpers import get_record_with_maximum_value, normalise_column
from otg.common.utils import convert_gnomad_position_to_ensembl
from otg.dataset.dataset import Dataset
from otg.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from otg.common.session import Session
    from otg.dataset.gene_index import GeneIndex


@dataclass
class VariantAnnotation(Dataset):
    """Dataset with variant-level annotations derived from GnomAD."""

    _schema: StructType = parse_spark_schema("variant_annotation.json")

    @classmethod
    def from_parquet(
        cls: type[VariantAnnotation], session: Session, path: str
    ) -> VariantAnnotation:
        """Initialise VariantAnnotation from parquet file.

        Args:
            session (Session): ETL session
            path (str): Path to parquet file

        Returns:
            VariantAnnotation: VariantAnnotation dataset
        """
        return super().from_parquet(session, path, cls._schema)

    @classmethod
    def from_gnomad(
        cls: type[VariantAnnotation],
        session: Session,
        gnomad_file: str,
        grch38_to_grch37_chain: str,
        populations: list,
        path: str | None = None,
    ) -> VariantAnnotation:
        """Generate variant annotation dataset from gnomAD.

        Some relevant modifications to the original dataset are:

        1. The transcript consequences features provided by VEP are filtered to only refer to the Ensembl canonical transcript.
        2. Genome coordinates are liftovered from GRCh38 to GRCh37 to keep as annotation.
        3. Field names are converted to camel case to follow the convention.

        Args:
            session (Session): ETL session
            gnomad_file (str): Path to `gnomad.genomes.vX.X.X.sites.ht` gnomAD dataset
            grch38_to_grch37_chain (str): Path to chain file for liftover
            populations (list): List of populations to include in the dataset
            path (Optional[str], optional): Path to save the dataset to. Defaults to None.

        Returns:
            VariantAnnotation: Variant annotation dataset
        """
        hl.init(sc=session.spark.sparkContext, log="/tmp/hail.log")

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
        grch38.add_liftover(grch38_to_grch37_chain, grch37)
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
                ).alias("variantId"),
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
        return cls(df=df, path=path)

    def max_maf(self: VariantAnnotation) -> Column:
        """Maximum minor allele frequency accross all populations.

        Returns:
            Column: Maximum minor allele frequency accross all populations.
        """
        return f.array_max(
            f.transform(
                self.df.alleleFrequencies,
                lambda af: f.when(
                    af.alleleFrequency > 0.5, 1 - af.alleleFrequency
                ).otherwise(af.alleleFrequency),
            )
        )

    def filter_by_variant_df(
        self: VariantAnnotation, df: DataFrame, cols: list[str]
    ) -> VariantAnnotation:
        """Filter variant annotation dataset by a variant dataframe.

        Args:
            df (DataFrame): A dataframe of variants
            cols (List[str]): A list of columns to join on

        Returns:
            VariantAnnotation: A filtered variant annotation dataset
        """
        self._df = self.df.join(f.broadcast(df.select(cols)), on=cols, how="inner")
        return self

    def get_transcript_consequence_df(
        self: VariantAnnotation, filter_by: GeneIndex = None
    ) -> DataFrame:
        """Dataframe of exploded transcript consequences.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            filter_by (GeneIndex): A gene index. Defaults to None.

        Returns:
            DataFrame: A dataframe exploded by transcript consequences
        """
        transript_consequences = self.df.select(
            "variantId",
            "chromosome",
            # exploding the array removes records without VEP annotation
            f.explode("vep.transcriptConsequences").alias("transcriptConsequence"),
        )
        if filter_by:
            transript_consequences = transript_consequences.join(
                f.broadcast(
                    filter_by.df.select(
                        f.col("id").alias("transcriptConsequence.gene_id")
                    )
                ),
                on="transcriptConsequence.gene_id",
            )
        return transript_consequences.persist()

    def get_most_severe_vep_v2g(
        self: VariantAnnotation,
        variant_consequence_lut_path: str,
        filter_by: GeneIndex = None,
    ) -> V2G:
        """Creates a dataset with variant to gene assignments based on VEP's predicted consequence on the transcript.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            variant_consequence_lut_path (str): Path to csv containing variant consequences sorted by severity
            filter_by (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: High and medium severity variant to gene assignments
        """
        variant_consequence_lut = self.etl.spark.read.csv(
            variant_consequence_lut_path, sep="\t", header=True
        ).select(
            f.element_at(f.split("Accession", r"/"), -1).alias(
                "variantFunctionalConsequenceId"
            ),
            f.col("Term").alias("label"),
            f.col("v2g_score").cast("double").alias("score"),
        )

        return V2G(
            df=self.get_transcript_consequence_df(filter_by)
            .select(
                "variantId",
                "chromosome",
                f.col("transcriptConsequence.gene_id").alias("geneId"),
                f.explode("transcriptConsequence.consequence_terms").alias("label"),
                f.lit("vep").alias("datatypeId"),
                f.lit("variantConsequence").alias("datasourceId"),
            )
            # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
            .join(
                f.broadcast(variant_consequence_lut),
                on="label",
                how="inner",
            )
            .filter(f.col("score") != 0)
            .transform(
                lambda df: get_record_with_maximum_value(
                    df, ["variantId", "geneId"], "score"
                )
            )
        )

    def get_polyphen_v2g(self: VariantAnnotation, filter_by: GeneIndex = None) -> V2G:
        """Creates a dataset with variant to gene assignments with a PolyPhen's predicted score on the transcript.

        Polyphen informs about the probability that a substitution is damaging. Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            filter_by (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: variant to gene assignments with their polyphen scores
        """
        return V2G(
            df=self.get_transcript_consequence_df(filter_by)
            .filter(f.col("transcriptConsequence.polyphen_score").isNotNull())
            .select(
                "variantId",
                "chromosome",
                f.col("transcriptConsequence.gene_id").alias("geneId"),
                f.col("transcriptConsequence.polyphen_score").alias("score"),
                f.col("transcriptConsequence.polyphen_prediction").alias("label"),
                f.lit("vep").alias("datatypeId"),
                f.lit("polyphen").alias("datasourceId"),
            )
        )

    def get_sift_v2g(self: VariantAnnotation, filter_by: GeneIndex = None) -> V2G:
        """Creates a dataset with variant to gene assignments with a SIFT's predicted score on the transcript.

        SIFT informs about the probability that a substitution is tolerated so scores nearer zero are more likely to be deleterious.
        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            filter_by (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: variant to gene assignments with their SIFT scores
        """
        return V2G(
            df=self.get_transcript_consequence_df(filter_by)
            .filter(f.col("transcriptConsequence.sift_score").isNotNull())
            .select(
                "variantId",
                "chromosome",
                f.col("transcriptConsequence.gene_id").alias("geneId"),
                f.expr("1 - transcriptConsequence.sift_score").alias("score"),
                f.col("transcriptConsequence.sift_prediction").alias("label"),
                f.lit("vep").alias("datatypeId"),
                f.lit("sift").alias("datasourceId"),
            )
        )

    def get_plof_v2g(self: VariantAnnotation, filter_by: GeneIndex = None) -> V2G:
        """Creates a dataset with variant to gene assignments with a flag indicating if the variant is predicted to be a loss-of-function variant by the LOFTEE algorithm.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            filter_by (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: variant to gene assignments from the LOFTEE algorithm
        """
        return V2G(
            df=self.get_transcript_consequence_df(filter_by)
            .filter(f.col("transcriptConsequence.lof").isNotNull())
            .withColumn(
                "isHighQualityPlof",
                f.when(f.col("transcriptConsequence.lof") == "HC", True).when(
                    f.col("transcriptConsequence.lof") == "LC", False
                ),
            )
            .withColumn(
                "score",
                f.when(f.col("isHighQualityPlof"), 1.0).when(
                    ~f.col("isHighQualityPlof"), 0
                ),
            )
            .select(
                f.col("id").alias("variantId"),
                "chromosome",
                f.col("transcriptConsequence.gene_id").alias("geneId"),
                "isHighQualityPlof",
                f.col("score"),
                f.lit("vep").alias("datatypeId"),
                f.lit("loftee").alias("datasourceId"),
            )
        )

    def get_distance_to_tss(
        self: VariantAnnotation,
        filter_by: GeneIndex,
        max_distance: int = 500_000,
    ) -> V2G:
        """Extracts variant to gene assignments for variants falling within a window of a gene's TSS.

        Args:
            filter_by (GeneIndex): A gene index to filter by.
            max_distance (int): The maximum distance from the TSS to consider. Defaults to 500_000.

        Returns:
            V2G: variant to gene assignments with their distance to the TSS
        """
        return V2G(
            df=self.df.alias("variant")
            .join(
                f.broadcast(filter_by.locations_lut()).alias("gene"),
                on=[
                    f.col("variant.chromosome") == f.col("gene.chromosome"),
                    f.abs(f.col("variant.position") - f.col("gene.tss"))
                    <= max_distance,
                ],
                how="inner",
            )
            .withColumn("inverse_distance", max_distance - f.col("distance"))
            .transform(lambda df: normalise_column(df, "inverse_distance", "score"))
            .select(
                "variantId",
                "chromosome",
                "position",
                "geneId",
                "score",
                f.lit("distance").alias("datatypeId"),
                f.lit("canonical_tss").alias("datasourceId"),
            )
        )
