"""Docs to create a dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING

from gentropy import Session

if TYPE_CHECKING:
    from gentropy import SummaryStatistics


def create_from_parquet(session: Session) -> SummaryStatistics:
    """Create a dataset from a path with parquet files."""
    # --8<-- [start:create_from_parquet_import]
    # Create a SummaryStatistics object by loading data from the specified path
    from gentropy import SummaryStatistics

    # --8<-- [end:create_from_parquet_import]

    path = "tests/gentropy/data_samples/sumstats_sample/GCST005523_chr18.parquet"
    # --8<-- [start:create_from_parquet]
    summary_stats = SummaryStatistics.from_parquet(session, path)
    # --8<-- [end:create_from_parquet]
    return summary_stats


def create_from_source(session: Session) -> SummaryStatistics:
    """Create a dataset from a path with parquet files."""
    # --8<-- [start:create_from_source_import]
    # Create a SummaryStatistics object by loading raw data from Finngen
    from gentropy.datasource.finngen.summary_stats import FinnGenSummaryStats

    # --8<-- [end:create_from_source_import]
    path = "tests/gentropy/data_samples/finngen_R9_AB1_ACTINOMYCOSIS.gz"
    # --8<-- [start:create_from_source]
    summary_stats = FinnGenSummaryStats.from_source(session.spark, path)
    # --8<-- [end:create_from_source]
    return summary_stats


def create_from_pandas(session: Session) -> SummaryStatistics:
    """Create a dataset from a path with Pandas files."""
    # --8<-- [start:create_from_pandas_import]
    import pandas as pd

    from gentropy import SummaryStatistics

    # --8<-- [end:create_from_pandas_import]

    path = "tests/gentropy/data_samples/sumstats_sample/GCST005523_chr18.parquet"
    custom_summary_stats_pandas_df = pd.read_parquet(path)
    # --8<-- [start:create_from_pandas]

    # Create a SummaryStatistics object specifying the data and schema
    custom_summary_stats_df = session.spark.createDataFrame(
        custom_summary_stats_pandas_df, schema=SummaryStatistics.get_schema()
    )
    custom_summary_stats = SummaryStatistics(_df=custom_summary_stats_df)
    # --8<-- [end:create_from_pandas]
    return custom_summary_stats
