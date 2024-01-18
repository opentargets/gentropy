#!/usr/bin/env python
"""A command line script to initiate single data source ingestion on Google Cloud Batch."""

from __future__ import annotations

import argparse
import logging

from cli_common import add_data_source_name_arg, resolve_data_source

parser = argparse.ArgumentParser()
add_data_source_name_arg(parser)

if __name__ == "__main__":
    # Parse command line arguments.
    args = parser.parse_args()
    # Look up data source class by its name.
    data_source = resolve_data_source(args.data_source_name)
    # Deploy code to Google Storage.
    data_source.deploy_code_to_storage()
    # Submit ingestion to Google Cloud Batch.
    data_source.submit()
    logging.info("Batch job has been submitted.")
