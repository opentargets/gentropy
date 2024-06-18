"""Dataset definition for variant annotation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as f

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import normalise_column
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

        # Rename columns to avoid conflicts:
        renamed_columns = {
            col: f"annotation_{col}" for col in annotation.columns if col != "variantId" else col
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
                f.array_union(
                    f.col("dbXrefs"),
                    f.col("annotation_dbXrefs"),
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

    def filter_by_variant_df(self: VariantIndex, df: DataFrame) -> VariantIndex:
        """Filter variant annotation dataset by a variant dataframe.

        Args:
            df (DataFrame): A dataframe of variants

        Returns:
            VariantIndex: A filtered variant annotation dataset
        """
        self.df = self._df.join(
            f.broadcast(df.select("variantId", "chromosome")),
            on=["variantId", "chromosome"],
            how="inner",
        )
        return self

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
            f.col("transcriptConsequence.geneId").alias("geneId"),
        )
        if gene_index:
            transript_consequences = transript_consequences.join(
                f.broadcast(gene_index.df),
                on=["chromosome", "geneId"],
            )
        return transript_consequences.persist()

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
