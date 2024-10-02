"""Dataset definition for variant annotation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict

import pyspark.sql.functions as f
import pyspark.sql.types as t

from gentropy.common.schemas import parse_spark_schema
from gentropy.common.spark_helpers import (
    get_nested_struct_schema,
    get_record_with_maximum_value,
    rename_all_columns,
    safe_array_union,
)
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame
    from pyspark.sql.types import StructType


@dataclass
class VariantIndex(Dataset):
    """Dataset for representing variants and methods applied on them."""

    class CONSEQUENCE_TO_PATHOGENICITY_SCORE_MAP(TypedDict):
        """Typing definition for CONSEQUENCE_TO_PATHOGENICITY_SCORE."""

        id: str
        label: str
        score: float

    CONSEQUENCE_TO_PATHOGENICITY_SCORE: list[CONSEQUENCE_TO_PATHOGENICITY_SCORE_MAP] = [
        {"id": "SO_0001575", "label": "splice_donor_variant", "score": 1.0},
        {"id": "SO_0001589", "label": "frameshift_variant", "score": 1.0},
        {"id": "SO_0001574", "label": "splice_acceptor_variant", "score": 1.0},
        {"id": "SO_0001587", "label": "stop_gained", "score": 1.0},
        {"id": "SO_0002012", "label": "start_lost", "score": 1.0},
        {"id": "SO_0001578", "label": "stop_lost", "score": 1.0},
        {"id": "SO_0001893", "label": "transcript_ablation", "score": 1.0},
        {"id": "SO_0001822", "label": "inframe_deletion", "score": 0.66},
        {"id": "SO_0001818", "label": "protein_altering_variant", "score": 0.66},
        {"id": "SO_0001821", "label": "inframe_insertion", "score": 0.66},
        {"id": "SO_0001787", "label": "splice_donor_5th_base_variant", "score": 0.66},
        {"id": "SO_0001583", "label": "missense_variant", "score": 0.66},
        {"id": "SO_0001567", "label": "stop_retained_variant", "score": 0.33},
        {"id": "SO_0001630", "label": "splice_region_variant", "score": 0.33},
        {"id": "SO_0002019", "label": "start_retained_variant", "score": 0.33},
        {
            "id": "SO_0002169",
            "label": "splice_polypyrimidine_tract_variant",
            "score": 0.33,
        },
        {"id": "SO_0001819", "label": "synonymous_variant", "score": 0.33},
        {"id": "SO_0002170", "label": "splice_donor_region_variant", "score": 0.33},
        {"id": "SO_0001624", "label": "3_prime_UTR_variant", "score": 0.1},
        {"id": "SO_0001623", "label": "5_prime_UTR_variant", "score": 0.1},
        {"id": "SO_0001627", "label": "intron_variant", "score": 0.1},
        {"id": "SO_0001619", "label": "non_coding_transcript_variant", "score": 0.0},
        {"id": "SO_0001580", "label": "coding_sequence_variant", "score": 0.0},
        {"id": "SO_0001632", "label": "downstream_gene_variant", "score": 0.0},
        {"id": "SO_0001631", "label": "upstream_gene_variant", "score": 0.0},
        {
            "id": "SO_0001792",
            "label": "non_coding_transcript_exon_variant",
            "score": 0.0,
        },
        {"id": "SO_0001620", "label": "mature_miRNA_variant", "score": 0.0},
    ]

    def __post_init__(self: VariantIndex) -> None:
        """Forcing the presence of empty arrays even if the schema allows missing values.

        To bring in annotations from other sources, we use the `array_union()` function. However it assumes
        both columns have arrays (not just the array schema!). If one of the array is null, the union
        is nullified. This needs to be avoided.
        """
        # Calling dataset's post init to validate schema:
        super().__post_init__()

        # Composing a list of expressions to replace nulls with empty arrays if the schema assumes:
        array_columns = {
            column.name: f.when(f.col(column.name).isNull(), f.array()).otherwise(
                f.col(column.name)
            )
            for column in self.df.schema
            if "ArrayType" in column.dataType.__str__()
        }

        # Not returning, but changing the data:
        self.df = self.df.withColumns(array_columns)

    @classmethod
    def get_schema(cls: type[VariantIndex]) -> StructType:
        """Provides the schema for the variant index dataset.

        Returns:
            StructType: Schema for the VariantIndex dataset
        """
        return parse_spark_schema("variant_index.json")

    @staticmethod
    def hash_long_variant_ids(
        variant_id: Column, chromosome: Column, position: Column, threshold: int = 100
    ) -> Column:
        """Hash long variant identifiers.

        Args:
            variant_id (Column): Column containing variant identifiers.
            chromosome (Column): Chromosome column.
            position (Column): position column.
            threshold (int): Above this limit, a hash will be generated.

        Returns:
            Column: Hashed variant identifiers for long variants.

        Examples:
            >>> (
            ...    spark.createDataFrame([('v_short', 'x', 23),('v_looooooong', '23', 23), ('no_chrom', None, None), (None, None, None)], ['variantId', 'chromosome', 'position'])
            ...    .select('variantId', VariantIndex.hash_long_variant_ids(f.col('variantId'), f.col('chromosome'), f.col('position'), 10).alias('hashedVariantId'))
            ...    .show(truncate=False)
            ... )
            +------------+--------------------------------------------+
            |variantId   |hashedVariantId                             |
            +------------+--------------------------------------------+
            |v_short     |v_short                                     |
            |v_looooooong|OTVAR_23_23_3749d019d645894770c364992ae70a05|
            |no_chrom    |OTVAR_41acfcd7d4fd523b33600b504914ef25      |
            |null        |null                                        |
            +------------+--------------------------------------------+
            <BLANKLINE>
        """
        return (
            # If either the position or the chromosome is missing, we hash the identifier:
            f.when(
                chromosome.isNull() | position.isNull(),
                f.concat(
                    f.lit("OTVAR_"),
                    f.md5(variant_id).cast("string"),
                ),
            )
            # If chromosome and position are given, but alleles are too long, create hash:
            .when(
                f.length(variant_id) > threshold,
                f.concat_ws(
                    "_",
                    f.lit("OTVAR"),
                    chromosome,
                    position,
                    f.md5(variant_id).cast("string"),
                ),
            )
            # Missing and regular variant identifiers are left unchanged:
            .otherwise(variant_id)
        )

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
        # Prefix for renaming columns:
        prefix = "annotation_"

        # Generate select expressions that to merge and import columns from annotation:
        select_expressions = []

        # Collect columns by iterating over the variant index schema:
        for field in VariantIndex.get_schema():
            column = field.name

            # If an annotation column can be found in both datasets:
            if (column in self.df.columns) and (column in annotation_source.df.columns):
                # Arrays are merged:
                if isinstance(field.dataType, t.ArrayType):
                    fields_order = None
                    if isinstance(field.dataType.elementType, t.StructType):
                        # Extract the schema of the array to get the order of the fields:
                        array_schema = [
                            field
                            for field in VariantIndex.get_schema().fields
                            if field.name == column
                        ][0].dataType
                        fields_order = get_nested_struct_schema(
                            array_schema
                        ).fieldNames()
                    select_expressions.append(
                        safe_array_union(
                            f.col(column), f.col(f"{prefix}{column}"), fields_order
                        ).alias(column)
                    )
                # Non-array columns are coalesced:
                else:
                    select_expressions.append(
                        f.coalesce(f.col(column), f.col(f"{prefix}{column}")).alias(
                            column
                        )
                    )
            # If the column is only found in the annotation dataset rename it:
            elif column in annotation_source.df.columns:
                select_expressions.append(f.col(f"{prefix}{column}").alias(column))
            # If the column is only found in the main dataset:
            elif column in self.df.columns:
                select_expressions.append(f.col(column))
            # VariantIndex columns not found in either dataset are ignored.

        # Join the annotation to the dataset:
        return VariantIndex(
            _df=(
                f.broadcast(self.df)
                .join(
                    rename_all_columns(annotation_source.df, prefix),
                    on=[f.col("variantId") == f.col(f"{prefix}variantId")],
                    how="left",
                )
                .select(*select_expressions)
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

    def get_distance_to_gene(
        self: VariantIndex,
        *,
        distance_type: str = "distanceFromTss",
        max_distance: int = 500_000,
    ) -> DataFrame:
        """Extracts variant to gene assignments for variants falling within a window of a gene's TSS or footprint.

        Args:
            distance_type (str): The type of distance to use. Can be "distanceFromTss" or "distanceFromFootprint". Defaults to "distanceFromTss".
            max_distance (int): The maximum distance to consider. Defaults to 500_000, the default window size for VEP.

        Returns:
            DataFrame: A dataframe with the distance between a variant and a gene's TSS or footprint.

        Raises:
            ValueError: Invalid distance type.
        """
        if distance_type not in {"distanceFromTss", "distanceFromFootprint"}:
            raise ValueError(
                f"Invalid distance_type: {distance_type}. Must be 'distanceFromTss' or 'distanceFromFootprint'."
            )
        df = self.df.select(
            "variantId", f.explode("transcriptConsequences").alias("tc")
        ).select("variantId", "tc.targetId", f"tc.{distance_type}")
        if max_distance == 500_000:
            return df
        elif max_distance < 500_000:
            return df.filter(f"{distance_type} <= {max_distance}")
        else:
            raise ValueError(
                f"max_distance must be less than 500_000. Got {max_distance}."
            )

    def get_loftee(self: VariantIndex) -> DataFrame:
        """Returns a dataframe with a flag indicating whether a variant is predicted to cause loss of function in a gene. The source of this information is the LOFTEE algorithm (https://github.com/konradjk/loftee).

        !!! note, "This will return a filtered dataframe with only variants that have been annotated by LOFTEE."

        Returns:
            DataFrame: variant to gene assignments from the LOFTEE algorithm
        """
        return (
            self.df.select("variantId", f.explode("transcriptConsequences").alias("tc"))
            .filter(f.col("tc.lofteePrediction").isNotNull())
            .withColumn(
                "isHighQualityPlof",
                f.when(f.col("tc.lofteePrediction") == "HC", True).when(
                    f.col("tc.lofteePrediction") == "LC", False
                ),
            )
            .select(
                "variantId",
                f.col("tc.targetId"),
                f.col("tc.lofteePrediction"),
                "isHighQualityPlof",
            )
        )

    def get_most_severe_gene_consequence(
        self: VariantIndex,
        *,
        vep_consequences: DataFrame,
    ) -> DataFrame:
        """Returns a dataframe with the most severe consequence for a variant/gene pair.

        Args:
            vep_consequences (DataFrame): A dataframe of VEP consequences

        Returns:
            DataFrame: A dataframe with the most severe consequence (plus a severity score) for a variant/gene pair
        """
        return (
            self.df.select("variantId", f.explode("transcriptConsequences").alias("tc"))
            .select(
                "variantId",
                f.col("tc.targetId"),
                f.explode(f.col("tc.variantFunctionalConsequenceIds")).alias(
                    "variantFunctionalConsequenceId"
                ),
            )
            .join(
                # TODO: make this table a project config
                f.broadcast(
                    vep_consequences.selectExpr(
                        "variantFunctionalConsequenceId", "score as severityScore"
                    )
                ),
                on="variantFunctionalConsequenceId",
                how="inner",
            )
            .filter(f.col("severityScore").isNull())
            .transform(
                # A variant can have multiple predicted consequences on a transcript, the most severe one is selected
                lambda df: get_record_with_maximum_value(
                    df, ["variantId", "targetId"], "severityScore"
                )
            )
            .withColumnRenamed(
                "variantFunctionalConsequenceId",
                "mostSevereVariantFunctionalConsequenceId",
            )
        )
