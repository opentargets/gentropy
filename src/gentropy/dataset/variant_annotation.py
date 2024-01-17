"""Variant annotation dataset."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    get_record_with_maximum_value,
    normalise_column,
)
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType

    from gentropy.dataset.gene_index import GeneIndex


@dataclass
class VariantAnnotation(Dataset):
    """Dataset with variant-level annotations."""

    @classmethod
    def get_schema(cls: type[VariantAnnotation]) -> StructType:
        """Provides the schema for the VariantAnnotation dataset.

        Returns:
            StructType: Schema for the VariantAnnotation dataset
        """
        return parse_spark_schema("variant_annotation.json")

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
        self: VariantAnnotation, df: DataFrame
    ) -> VariantAnnotation:
        """Filter variant annotation dataset by a variant dataframe.

        Args:
            df (DataFrame): A dataframe of variants

        Returns:
            VariantAnnotation: A filtered variant annotation dataset
        """
        self.df = self._df.join(
            f.broadcast(df.select("variantId", "chromosome")),
            on=["variantId", "chromosome"],
            how="inner",
        )
        return self

    def get_transcript_consequence_df(
        self: VariantAnnotation, gene_index: GeneIndex | None = None
    ) -> DataFrame:
        """Dataframe of exploded transcript consequences.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            gene_index (GeneIndex | None): A gene index. Defaults to None.

        Returns:
            DataFrame: A dataframe exploded by transcript consequences with the columns variantId, chromosome, transcriptConsequence
        """
        # exploding the array removes records without VEP annotation
        transript_consequences = self.df.withColumn(
            "transcriptConsequence", f.explode("vep.transcriptConsequences")
        ).select(
            "variantId",
            "chromosome",
            "position",
            "transcriptConsequence",
            f.col("transcriptConsequence.geneId").alias("geneId"),
        )
        if gene_index:
            transript_consequences = transript_consequences.join(
                f.broadcast(gene_index.df),
                on=["chromosome", "geneId"],
            )
        return transript_consequences.persist()

    def get_most_severe_vep_v2g(
        self: VariantAnnotation,
        vep_consequences: DataFrame,
        gene_index: GeneIndex,
    ) -> V2G:
        """Creates a dataset with variant to gene assignments based on VEP's predicted consequence of the transcript.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            vep_consequences (DataFrame): A dataframe of VEP consequences
            gene_index (GeneIndex): A gene index to filter by. Defaults to None.

        Returns:
            V2G: High and medium severity variant to gene assignments
        """
        return V2G(
            _df=self.get_transcript_consequence_df(gene_index)
            .select(
                "variantId",
                "chromosome",
                f.col("transcriptConsequence.geneId").alias("geneId"),
                f.explode("transcriptConsequence.consequenceTerms").alias("label"),
                f.lit("vep").alias("datatypeId"),
                f.lit("variantConsequence").alias("datasourceId"),
            )
            .join(
                f.broadcast(vep_consequences),
                on="label",
                how="inner",
            )
            .drop("label")
            .filter(f.col("score") != 0)
            # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
            .transform(
                lambda df: get_record_with_maximum_value(
                    df, ["variantId", "geneId"], "score"
                )
            ),
            _schema=V2G.get_schema(),
        )

    def get_plof_v2g(self: VariantAnnotation, gene_index: GeneIndex) -> V2G:
        """Creates a dataset with variant to gene assignments with a flag indicating if the variant is predicted to be a loss-of-function variant by the LOFTEE algorithm.

        Optionally the trancript consequences can be reduced to the universe of a gene index.

        Args:
            gene_index (GeneIndex): A gene index to filter by.

        Returns:
            V2G: variant to gene assignments from the LOFTEE algorithm
        """
        return V2G(
            _df=(
                self.get_transcript_consequence_df(gene_index)
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
                    "variantId",
                    "chromosome",
                    "geneId",
                    "isHighQualityPlof",
                    f.col("score"),
                    f.lit("vep").alias("datatypeId"),
                    f.lit("loftee").alias("datasourceId"),
                )
            ),
            _schema=V2G.get_schema(),
        )

    def get_distance_to_tss(
        self: VariantAnnotation,
        gene_index: GeneIndex,
        max_distance: int = 500_000,
    ) -> V2G:
        """Extracts variant to gene assignments for variants falling within a window of a gene's TSS.

        Args:
            gene_index (GeneIndex): A gene index to filter by.
            max_distance (int): The maximum distance from the TSS to consider. Defaults to 500_000.

        Returns:
            V2G: variant to gene assignments with their distance to the TSS
        """
        return V2G(
            _df=(
                self.df.alias("variant")
                .join(
                    f.broadcast(gene_index.locations_lut()).alias("gene"),
                    on=[
                        f.col("variant.chromosome") == f.col("gene.chromosome"),
                        f.abs(f.col("variant.position") - f.col("gene.tss"))
                        <= max_distance,
                    ],
                    how="inner",
                )
                .withColumn(
                    "distance", f.abs(f.col("variant.position") - f.col("gene.tss"))
                )
                .withColumn(
                    "inverse_distance",
                    max_distance - f.col("distance"),
                )
                .transform(lambda df: normalise_column(df, "inverse_distance", "score"))
                .select(
                    "variantId",
                    f.col("variant.chromosome").alias("chromosome"),
                    "distance",
                    "geneId",
                    "score",
                    f.lit("distance").alias("datatypeId"),
                    f.lit("canonical_tss").alias("datasourceId"),
                )
            ),
            _schema=V2G.get_schema(),
        )
