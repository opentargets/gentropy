"""Dataset definition for variant annotation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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
class VariantIndex(Dataset):
    """Dataset for representing variants and methods applied on them."""

    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_index.json")

    @classmethod
    def assign_variant_id(
        cls: type[VariantIndex],
    ) -> Column:
        """Creates a column with the variant ID that will be used to index the variant index.

        This is to ensure that the variant ID is unique and not too long.

        Returns:
            Column: Column with the variant ID containing the hash if the variant ID is longer than 100 characters
        """
        return (
            f.when(
                f.length(f.col("variantId")) >= 100,
                f.concat(
                    f.lit("otvar_"),
                    f.xxhash64(f.col("variantId")).cast("string"),
                ),
            )
            .otherwise(f.col("variantId"))
            .alias("variantId")
        )

    @staticmethod
    def fetch_coordinates(
        rsids: list[str], batch_size: int = 200, pause_time: int = 1
    ) -> dict[str, list[str]]:
        """Batch query the Ensembl API to extract variant coordinates from a list of rsIds.

        Args:
            rsids (list[str]): List of rsIDs
            batch_size (int): Number of rsIDs to process in each batch.
            pause_time (int): Time to pause between batches.

        Returns:
            dict[str, list[str]]: Dictionary with rsID as key and list of variant IDs as value.

        Raises:
            Exception: If an error occurs while processing the batches.

        Example:
            >>> VariantIndex.fetch_coordinates(["rs75493593"])
            [{'rs75493593': ['17_7041768_G_C', '17_7041768_G_T']}]
        """

        def _ensembl_batch_request(rsids: list[str]) -> dict[str, dict[str, Any]]:
            """Access the batch endpoint of Ensembl.

            Args:
                rsids (list[str]): List of rsIDs

            Returns:
                dict[str, dict[str, Any]]: Dictionary with rsID as key and variant data as value.
            """
            url = "https://rest.ensembl.org/variation/human"
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            data = {"ids": rsids}

            return requests.post(f"{url}", headers=headers, json=data).json()

        def _parse_response(
            response: dict[str, dict[str, Any]],
        ) -> dict[str, list[str]]:
            """Parse the response from the Ensembl API.

            Args:
                response (dict[str, dict[str, Any]]): Response from the Ensembl API. This is a dictionary where the key is the rsID and the value is the variant data.

            Returns:
                dict[str, list[str]]: Dictionary with rsID as key and list of variant IDs as value.
            """
            parsed_results = {}
            valid_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]
            for rsid in response:
                if response[rsid]["mappings"]:
                    for data in response[rsid]["mappings"]:
                        if data["seq_region_name"] in valid_chromosomes:
                            chrom = data["seq_region_name"]
                            pos = data["start"]
                            # The first element of allele_string contains the reference allele and the rest are the alternate alleles
                            ref_allele = data["allele_string"].split("/")[0]
                            alt_alleles = data["allele_string"].split("/")[1:]
                            variant_ids = [
                                f"{chrom}_{pos}_{ref_allele}_{alt_allele}"
                                for alt_allele in alt_alleles
                            ]
                            parsed_results[rsid] = variant_ids
                else:
                    continue
            return parsed_results

        import time

        import requests

        all_results = {}

        # Chunk the rsids into batches
        for i in range(0, len(rsids), batch_size):
            batch = rsids[i : i + batch_size]
            try:
                variant_data = _ensembl_batch_request(batch)
                all_results.update(_parse_response(variant_data))

            except Exception as e:
                raise Exception(
                    f"Error processing batch {i // batch_size + 1}: {e}"
                ) from e

            time.sleep(pause_time)

        return all_results

    def add_annotation(
        self: VariantIndex, annotation_source: VariantIndex
    ) -> VariantIndex:
        """Import annotation from an other variant index dataset.

        At this point the annotation can be extended with extra cross-references,
        in-silico predictions and allele frequencies.

        Args:
            annotation_source (VariantIndex): Annotation to add to the dataset

        Returns:
            VariantIndex: VariantIndex dataset with the annotation added
        """
        # Columns in the source dataset:
        variant_index_columns = [
            # Combining cross-references:
            f.array_union(f.col("dbXrefs"), f.col("annotation_dbXrefs"))
            if row == "dbXrefs"
            # Combining in silico predictors:
            else f.array_union(
                f.col("inSilicoPredictors"), f.col("annotation_inSilicoPredictors")
            )
            if row == "inSilicoPredictors"
            # Combining allele frequencies:
            else f.array_union(
                f.col("alleleFrequencies"), f.col("annotation_alleleFrequencies")
            )
            if row == "alleleFrequencies"
            # Carrying over all other columns:
            else row
            for row in self.df.columns
        ]

        # Rename columns in the annotation source to avoid conflicts:
        annotation = annotation_source.df.select(
            *[
                f.col(col).alias(f"annotation_{col}") if col != "variantId" else col
                for col in annotation_source.df.columns
            ]
        )

        # Join the annotation to the dataset:
        return VariantIndex(
            _df=(
                annotation.join(
                    f.broadcast(self.df), on="variantId", how="right"
                ).select(*variant_index_columns)
            ),
            _schema=self.schema,
        )

    def max_maf(self: VariantIndex) -> Column:
        """Maximum minor allele frequency accross all populations assuming all variants biallelic.

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
                f.explode(
                    "transcriptConsequence.variantFunctionalConsequenceIds"
                ).alias("variantFunctionalConsequenceId"),
                f.lit("vep").alias("datatypeId"),
                f.lit("variantConsequence").alias("datasourceId"),
            )
            .join(
                f.broadcast(vep_consequences),
                on="variantFunctionalConsequenceId",
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
