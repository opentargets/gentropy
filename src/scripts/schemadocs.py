"""Script to generate schema assets for mkdocs documentation."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyspark.sql.types as t
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]").appName("schemas").getOrCreate()


def generate_schema_assets(assets_dir: Path, schema_dir: str) -> None:
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
                with (assets_dir / outfilename).open("w") as out:
                    tree = df._jdf.schema().treeString()
                    out.write(f"```\n{tree}\n```")


def main(config: dict) -> None:
    """Main function."""
    # Create schema dir if not exist:
    assets_dir = Path("docs/assets/schemas")
    assets_dir.mkdir(exist_ok=True)

    generate_schema_assets(
        assets_dir=assets_dir,
        schema_dir="src/otg/json/schemas",
    )
    print(f"Schema assests generated for {config['site_name']}")
