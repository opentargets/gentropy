"""Utilities shared between CLI scripts."""

from __future__ import annotations

import argparse

from batch_common import DataSourceBase
from data_sources import data_source_look_up


def add_data_source_name_arg(argument_parser: argparse.ArgumentParser) -> None:
    """Add an data source selection argument to the argument parser.

    Args:
        argument_parser (argparse.ArgumentParser): The argument parser to which to add the argument.
    """
    argument_parser.add_argument(
        "data_source_name",
        type=str,
        choices=data_source_look_up.keys(),
        help="Name of data source to ingest.",
    )


def resolve_data_source(data_source_name: str) -> DataSourceBase:
    """Resolve a data source class on its name.

    Args:
        data_source_name (str): Name of the data source.

    Returns:
        DataSourceBase: The corresponding data source class.
    """
    assert (
        data_source_name in data_source_look_up
    ), f"Could not resolve data source {data_source_name} based on available data sources: {data_source_look_up}"
    data_source = data_source_look_up[data_source_name]
    return data_source()
