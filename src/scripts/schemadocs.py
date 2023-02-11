"""Script to generate schema assets for mkdocs documentation."""

from __future__ import annotations

import json
import os

import pyspark.sql.types as t
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("schemas").getOrCreate()


def generate_schema_assets(assets_dir: str, schema_dir: str) -> None:
    """Generate schema assets for mkdocs documentation.

    Args:
        assets_dir: Path to assets directory.
        schema_dir: Path to schema directory.
    """
    for i in os.listdir(schema_dir):
        if i.endswith(".json"):
            with open(f"{schema_dir}/{i}") as f:
                d = json.load(f)
                input_schema = t.StructType.fromJson(d)
                df = spark.createDataFrame([], input_schema)
                outfilename = i.replace("json", "md")
                with open(f"{assets_dir}/{outfilename}", "w") as out:
                    tree = df._jdf.schema().treeString()
                    out.write(f"```\n{tree}\n```")


def main(config: dict) -> None:
    """Main function."""
    generate_schema_assets(
        assets_dir="docs/assets/schemas", schema_dir="src/otg/json/schemas"
    )
    print(f"Schema assests generated for {config['site_name']}")
