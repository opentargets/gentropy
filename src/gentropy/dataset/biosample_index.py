"""Biosample index dataset."""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType

from gentropy.common.schemas import parse_spark_schema
from gentropy.dataset.dataset import Dataset

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass
class BiosampleIndex(Dataset):
    """Biosample index dataset.

    A Biosample index dataset captures the metadata of the biosamples (e.g. tissues, cell types, cell lines, etc) such as alternate names and relationships with other biosamples.
    """

    @classmethod
    def get_schema(cls: type[BiosampleIndex]) -> StructType:
        """Provide the schema for the BiosampleIndex dataset.

        Returns:
            StructType: The schema of the BiosampleIndex dataset.
        """
        return parse_spark_schema("biosample_index.json")

    def merge_indices(
        self: BiosampleIndex,
        biosample_indices : list[BiosampleIndex]
        ) -> BiosampleIndex:
        """Merge a list of biosample indices into a single biosample index.

        Where there are conflicts, in single values - the first value is taken. In list values, the union of all values is taken.

        Args:
            biosample_indices (list[BiosampleIndex]): Biosample indices to merge.

        Returns:
            BiosampleIndex: Merged biosample index.
        """
        # Extract the DataFrames from the BiosampleIndex objects
        biosample_dfs = [biosample_index.df for biosample_index in biosample_indices] + [self.df]

        # Merge the DataFrames
        merged_df = reduce(DataFrame.unionAll, biosample_dfs)

        # Determine aggregation functions for each column
        # Currently this will take the first value for single values and merge lists for list values
        agg_funcs = []
        for field in merged_df.schema.fields:
            if field.name != "biosampleId":  # Skip the grouping column
                if field.dataType == ArrayType(StringType()):
                    agg_funcs.append(f.array_distinct(f.flatten(f.collect_list(field.name))).alias(field.name))
                else:
                    agg_funcs.append(f.first(f.col(field.name), ignorenulls=True).alias(field.name))

        # Perform aggregation
        aggregated_df = merged_df.groupBy("biosampleId").agg(*agg_funcs)

        return BiosampleIndex(
            _df=aggregated_df,
            _schema=BiosampleIndex.get_schema()
            )

    def retain_rows_with_ancestor_id(
        self: BiosampleIndex,
        ancestor_ids : list[str]
        ) -> BiosampleIndex:
        """Filter the biosample index to retain only rows with the given ancestor IDs.

        Args:
            ancestor_ids (list[str]): Ancestor IDs to filter on.

        Returns:
            BiosampleIndex: Filtered biosample index.
        """
        # Create a Spark array of ancestor IDs prior to filtering
        ancestor_ids_array = f.array(*[f.lit(id) for id in ancestor_ids])

        return BiosampleIndex(
            _df=self.df.filter(
                f.size(f.array_intersect(f.col("ancestors"), ancestor_ids_array)) > 0
            ),
            _schema=BiosampleIndex.get_schema()
            )
