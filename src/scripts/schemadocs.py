"""Script to generate schema assets for mkdocs documentation."""

from __future__ import annotations

import json
import os
from pathlib import Path

import mkdocs.plugins
import pyspark.sql.types as t
from mkdocs.config import Config as MkdocsConfig
from pyspark.sql import SparkSession


def spark_connect() -> SparkSession:
    """Create SparkSession.

    Returns:
        SparkSession: SparkSession object.
    """
    spark = SparkSession.builder.master("local[1]").appName("schemas").getOrCreate()
    return spark


def generate_schema_assets(
    spark: SparkSession, assets_dir: Path, schema_dir: str
) -> None:
    """Generate schema assets for mkdocs documentation.

    Args:
        spark: SparkSession object.
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
                    tree = df._jdf.schema().treeString()  # type: ignore
                    out.write(f"```\n{tree}\n```")


@mkdocs.plugins.event_priority(50)
def on_pre_build(config: MkdocsConfig, **kwargs) -> None:
    """Main function.

    Args:
        config: MkdocsConfig object.
        **kwargs: Arbitrary keyword arguments.
    """
    # Create schema dir if not exist:
    assets_dir = Path("docs/assets/schemas")
    assets_dir.mkdir(exist_ok=True)

    spark = spark_connect()
    generate_schema_assets(
        spark=spark,
        assets_dir=assets_dir,
        schema_dir="src/otg/assets/schemas",
    )
    print(f"Schema assets generated for {config['site_name']}")
