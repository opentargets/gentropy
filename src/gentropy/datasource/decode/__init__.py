"""deCODE proteomics datasource module."""

from __future__ import annotations

from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.external.s3 import S3Config

# 1. Build the studyIndex from the manifest
# 2. Download the raw deCODE data files
# 3. Download the smp deCODE data files
# 4. Convert to parquet
# 5. Harmonisation to standard format
# 6. QualityControls


class deCODEDataSource(str, Enum):
    """deCODE proteomics data sources."""

    DECODE_PROTEOMICS_RAW = "deCODE-proteomics-raw"
    DECODE_PROTEOMICS_SMP = "deCODE-proteomics-smp"


class deCODEManifest:
    """deCODE manifest class."""

    bucket_listing_schema = t.StructType(
        [
            t.StructField("date", t.DateType()),
            t.StructField("time", t.StringType()),
            t.StructField("empty", t.StringType()),  # empty field in aws s3 ls output
            t.StructField("size", t.StringType()),
            t.StructField("unit", t.StringType()),
            t.StructField("relativePath", t.StringType()),
        ]
    )

    def __init__(self, df: DataFrame) -> None:
        """Initialize deCODE manifest."""
        self.df = df

    def get_summmary_statistics_paths(self) -> list[str]:
        """Get summary statistics paths from manifest.

        Returns:
            list[str]: List of summary statistics paths.
        """
        return [
            row.summarystatsLocation
            for row in self.df.select("summarystatsLocation").collect()
        ]

    @classmethod
    def from_path(cls, session: Session, path: str) -> deCODEManifest:
        """Load deCODE manifest from parquet file.

        Args:
            session (Session): Gentropy session.
            path (str): Path to the manifest file.

        Returns:
            deCODEManifest: deCODE manifest instance.
        """
        manifest_df = session.spark.read.parquet(path)
        return cls(df=manifest_df)

    @classmethod
    def from_bucket_listing(
        cls, session: Session, path: str, config: S3Config
    ) -> deCODEManifest:
        r"""Create deCODE manifest from listing s3 compatible bucket.

        The command to list the bucket:
        ```
        aws s3 ls \\
          --recursive \\
          --human-readable \\
          --summarize  \\
          --profile $1 \\
          $2 \\
          --endpoint-url https://${S3_HOST_URL}:${S3_HOST_PORT} > manifest.txt
        ```
        The output manifest.txt file can then be used to create the deCODEManifest.
        Note that the bottom summary lines should be removed from the manifest file before use.

        Args:
            session (Session): Gentropy session.
            path (str): Path to the manifest file.
            config (S3Config): S3 configuration.

        Returns:
            deCODEManifest: deCODE manifest instance.
        """
        project_id = f.when(
            f.col("relativePath").contains("SMP"),
            f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
        ).otherwise(deCODEDataSource.DECODE_PROTEOMICS_RAW.value)

        manifest_df = (
            session.spark.read.csv(
                path, sep=" ", header=True, schema=cls.bucket_listing_schema
            )
            .drop("empty")
            .withColumn("projectId", project_id)
            .withColumn(
                "studyId",
                f.concat_ws(
                    "_",
                    f.col("projectId"),
                    f.regexp_extract(
                        "relativePath", r"^.*/(Proteomics_.*)\.txt.gz$", 1
                    ),
                ),
            )
            .withColumn("hasSumstats", f.lit(True))
            .withColumn(
                "summarystatsLocation",
                f.concat(
                    f.lit("s3a://"),
                    f.lit(config.bucket_name),
                    f.lit("/"),
                    f.col("relativePath"),
                ),
            )
            .withColumn("size", f.concat_ws(" ", f.col("size"), f.col("unit")))
            .withColumn(
                "accessionTimestamp",
                f.to_timestamp(
                    f.concat_ws(" ", f.col("date"), f.col("time")),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
            .select(
                "projectId",
                "studyId",
                "hasSumstats",
                "summarystatsLocation",
                "size",
                "accessionTimestamp",
            )
        )
        return cls(df=manifest_df)
