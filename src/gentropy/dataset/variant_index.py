"""Dataset definition for variant annotation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    get_record_with_maximum_value,
    normalise_column,
)
from gentropy.dataset.dataset import Dataset
from gentropy.dataset.gene_index import GeneIndex
from gentropy.dataset.v2g import V2G

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


@dataclass
class VariantIndex(Dataset):
    """Variant annotation object based on the parsed VEP output."""

    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the variant annotation dataset.

        Returns:
            StructType: Schema for the VariantAnnotationNew dataset
        """
        return parse_spark_schema("variant_index.json")

    def add_annotation(self: VariantIndex, annotation: DataFrame) -> VariantIndex:
        """Add annotation to the dataset.

        At this point the variant annotation can be extended with extra cross-references and allele frequencies.

        Args:
            annotation (DataFrame): Annotation to add to the dataset

        Returns:
            VariantIndex: VariantIndex dataset with the annotation added
        """
        # Assert that the provided annotation has unique variant identifiers:
        assert (
            annotation.select("variantId").distinct().count() == annotation.count()
        ), "Annotation must have unique variant identifiers."

        # Rename columns to avoid conflicts:s
        renamed_columns = {
            col: f"annotation_{col}" if col != "variantId" else col
            for col in annotation.columns
        }
        annotation = annotation.select(
            *[f.col(col).alias(renamed_columns[col]) for col in annotation.columns]
        )
        # Join the annotation to the dataset:
        jonied = self.df.join(annotation, on="variantId", how="left")

        # Merge cross-ref if present:
        if "dbXrefs" in renamed_columns:
            jonied = jonied.withColumn(
                "dbXrefs",
                f.when(
                    f.col("dbXrefs").isNotNull()
                    & f.col("annotation_dbXrefs").isNotNull(),
                    f.array_union(
                        f.col("dbXrefs"),
                        f.col("annotation_dbXrefs"),
                    ),
                ).otherwise(
                    f.coalesce(
                        f.col("dbXrefs"),
                        f.col("annotation_dbXrefs"),
                    )
                ),
            )

        # Rename population column if not present in the dataset:
        if ("alleleFrequencies" in renamed_columns) and (
            "alleleFrequencies" not in jonied.columns
        ):
            jonied = jonied.withColumnRenamed(
                "annotation_alleleFrequencies",
                "alleleFrequencies",
            )

        # Drop the annotation columns:
        jonied = jonied.drop(*[col for col in jonied.columns if "annotation_" in col])

        # Join the annotation to the dataset:
        return VariantIndex(
            _df=jonied,
            _schema=self.schema,
        )

    def max_maf(self: VariantIndex) -> Column:
        """Maximum minor allele frequency accross all populations.

        Returns:
            Column: Maximum minor allele frequency accross all populations.

        Raises:
            ValueError: Allele frequencies are not present in the dataset.
        """
        if "alleleFrequencies" not in self.df.columns:
            raise ValueError("Allele frequencies are not present in the dataset.")

        return f.array_max(
            f.transform(
                self.df.alleleFrequencies,
                lambda af: f.when(
                    af.alleleFrequency > 0.5, 1 - af.alleleFrequency
                ).otherwise(af.alleleFrequency),
            )
        )

    def filter_by_variant(self: VariantIndex, df: DataFrame) -> VariantIndex:
        """Filter variant annotation dataset by a variant dataframe.

        Args:
            df (DataFrame): A dataframe of variants

        Returns:
            VariantIndex: A filtered variant annotation dataset
        """
        join_columns = ["variantId", "chromosome"]

        assert all(
            col in df.columns for col in join_columns
        ), "The variant dataframe must contain the columns 'variantId' and 'chromosome'."

        return VariantIndex(
            _df=self._df.join(
                f.broadcast(df.select(*join_columns).distinct()),
                on=join_columns,
                how="inner",
            ),
            _schema=self.schema,
        )

    def get_transcript_consequence_df(
        self: VariantIndex, gene_index: GeneIndex | None = None
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
            "transcriptConsequence", f.explode("transcriptConsequences")
        ).select(
            "variantId",
            "chromosome",
            "position",
            "transcriptConsequence",
            f.col("transcriptConsequence.targetId").alias("geneId"),
        )
        if gene_index:
            transript_consequences = transript_consequences.join(
                f.broadcast(gene_index.df),
                on=["chromosome", "geneId"],
            )
        return transript_consequences

    def get_distance_to_tss(
        self: VariantIndex,
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

    def get_plof_v2g(self: VariantIndex, gene_index: GeneIndex) -> V2G:
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
                .filter(f.col("transcriptConsequence.lofteePrediction").isNotNull())
                .withColumn(
                    "isHighQualityPlof",
                    f.when(
                        f.col("transcriptConsequence.lofteePrediction") == "HC", True
                    ).when(
                        f.col("transcriptConsequence.lofteePrediction") == "LC", False
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

    def get_most_severe_transcript_consequence(
        self: VariantIndex,
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
                f.col("transcriptConsequence.targetId").alias("geneId"),
                f.explode("transcriptConsequence.variantConsequenceIds").alias(
                    "variantConsequenceId"
                ),
                f.lit("vep").alias("datatypeId"),
                f.lit("variantConsequence").alias("datasourceId"),
            )
            .join(
                f.broadcast(vep_consequences),
                on="variantConsequenceIds",
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
