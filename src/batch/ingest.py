#!/usr/bin/env python
"""A command line script to ingest one file from one data source on Google Cloud infrastructure."""

from __future__ import annotations

import argparse

from cli_common import add_data_source_name_arg, resolve_data_source

parser = argparse.ArgumentParser()
add_data_source_name_arg(parser)
parser.add_argument(
    "task_index",
    type=int,
    help="Index of the current task across all tasks in the batch",
)

if __name__ == "__main__":
    # Parse command line arguments.
    args = parser.parse_args()
    # Look up data source class by its name.
    data_source = resolve_data_source(args.data_source_name)
    # Process the specified input file.
    data_source.ingest(args.task_index)
