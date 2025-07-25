"""Docs to inspect a dataset."""
from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy import SummaryStatistics

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


def filter_dataset(summary_stats: SummaryStatistics) -> SummaryStatistics:
    """Docs to filter the `df` attribute of a dataset using Dataset.filter."""
    # --8<-- [start:filter_dataset]
    import pyspark.sql.functions as f

    # Filter summary statistics to only include associations in chromosome 22
    filtered = summary_stats.filter(condition=f.col("chromosome") == "22")
    # --8<-- [end:filter_dataset]
    return filtered


def interact_w_dataframe(summary_stats: SummaryStatistics) -> SummaryStatistics:
    """Docs to interact with the `df` attribute of a dataset."""
    # --8<-- [start:print_dataframe]
    # Inspect the first 10 rows of the data
    summary_stats.df.show(10)
    # --8<-- [end:print_dataframe]

    # --8<-- [start:print_dataframe_schema]
    # Print the schema of the data
    summary_stats.df.printSchema()
    # --8<-- [end:print_dataframe_schema]
    return summary_stats


def get_dataset_schema(summary_stats: SummaryStatistics) -> StructType:
    """Docs to get the schema of a dataset."""
    # --8<-- [start:get_dataset_schema]
    # Get the Spark schema of any `Dataset` as a `StructType` object
    schema = summary_stats.get_schema()
    # --8<-- [end:get_dataset_schema]
    return schema


def write_data(summary_stats: SummaryStatistics) -> None:
    """Docs to write a dataset to disk."""
    # --8<-- [start:write_parquet]
    # Write the data to disk in parquet format
    summary_stats.df.write.parquet("path/to/summary/stats")
    # --8<-- [end:write_parquet]

    # --8<-- [start:write_csv]
    # Write the data to disk in csv format
    summary_stats.df.write.csv("path/to/summary/stats")
    # --8<-- [end:write_csv]
