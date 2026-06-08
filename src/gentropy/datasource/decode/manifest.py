"""deCODE ingestion manifest.

The `deCODEManifest` dataset catalogues every summary-statistics file available
in the deCODE S3 bucket. It is generated once from an ``aws s3 ls`` bucket listing and
subsequently consumed by downstream ingestion steps to locate per-study TSV gzip files
and to associate each assay with a project identifier.
"""

from __future__ import annotations

from pyspark.sql import functions as f
from pyspark.sql import types as t

from gentropy import Session
from gentropy.dataset.dataset import Dataset
from gentropy.datasource.decode import deCODEDataSource
from gentropy.external.s3 import S3Config


class deCODEManifest(Dataset):
    """Catalogue of deCODE summary-statistics files derived from an S3 bucket listing.

    Each row corresponds to one SomaScan assay and records the S3 path of its
    gzipped TSV summary-statistics file together with provenance metadata such as
    file size and accession timestamp.  The `studyId` follows the convention
    ``{projectId}_{Proteomics_*}`` as embedded in the file path.

    The manifest is produced by `from_bucket_listing` and later consumed by
    `deCODEStudyIndex` and `deCODESummaryStatistics`.
    """

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

    @classmethod
    def get_schema(cls) -> t.StructType:
        """Return the enforced Spark schema for `deCODEManifest`.

        Returns:
            t.StructType: Schema with fields ``projectId``, ``studyId``,
                ``hasSumstats``, ``summarystatsLocation``, ``size``, and
                ``accessionTimestamp``.
        """
        return t.StructType(
            [
                t.StructField("projectId", t.StringType()),
                t.StructField("studyId", t.StringType()),
                t.StructField("hasSumstats", t.BooleanType()),
                t.StructField("summarystatsLocation", t.StringType()),
                t.StructField("size", t.StringType()),
                t.StructField("accessionTimestamp", t.TimestampType()),
            ]
        )

    def get_summary_statistics_paths(self) -> list[str]:
        """Get summary statistics paths from manifest.

        Returns:
            list[str]: List of summary statistics paths.

        Examples:
            >>> data = [("path1",), ("path2",)]
            >>> schema = "summarystatsLocation STRING"
            >>> df = spark.createDataFrame(data, schema)
            >>> manifest = deCODEManifest(df)
            >>> manifest.get_summary_statistics_paths()
            ['path1', 'path2']
        """
        return [
            row.summarystatsLocation
            for row in self.df.select("summarystatsLocation").collect()
        ]

    @classmethod
    def from_bucket_listing(
        cls,
        session: Session,
        path: str,
        s3_config_path: str | None = None,
    ) -> deCODEManifest:
        r"""Create a `deCODEManifest` from an ``aws s3 ls`` bucket listing file.

        The listing file must be produced with the following command (bottom summary
        lines should be removed before ingestion):

        ```bash
        aws s3 ls \\
          --recursive \\
          --human-readable \\
          --summarize  \\
          --profile $1 \\
          $2 \\
          --endpoint-url https://${S3_HOST_URL}:${S3_HOST_PORT} | grep "Proteomics" > manifest.txt
        ```

        The ``s3_config_path`` JSON/YAML file must contain the S3 bucket name so that
        fully-qualified ``s3a://`` paths can be constructed for each file.

        Args:
            session (Session): Active Gentropy Spark session.
            path (str): Path to the ``aws s3 ls`` output text file.
            s3_config_path (str | None): Path to the S3 configuration file (used to resolve
                the bucket name for constructing absolute ``s3a://`` URIs).

        Returns:
            deCODEManifest: Populated manifest dataset.
        """
        config = S3Config.read(s3_config_path)
        project_id = f.when(
            f.col("relativePath").contains("Proteomics_SMP_"),
            f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
        ).otherwise(deCODEDataSource.DECODE_PROTEOMICS_RAW.value)

        manifest_df = (
            session.spark.read.csv(
                path,
                sep=" ",
                header=False,
                schema=cls.bucket_listing_schema,
            )
            .drop("empty")
            .withColumn("projectId", project_id)
            .withColumn(
                "studyId",
                f.concat_ws(
                    "_",
                    f.col("projectId"),
                    f.regexp_extract(
                        "relativePath",
                        r"^.*/(Proteomics_.*)\.txt.gz$",
                        1,
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
        return cls(_df=manifest_df)
