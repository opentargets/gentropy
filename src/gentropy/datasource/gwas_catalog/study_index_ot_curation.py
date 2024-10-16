"""Study Index for GWAS Catalog data source."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark import SparkFiles

from gentropy.common.session import Session

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass
class StudyIndexGWASCatalogOTCuration:
    """Study Index Curation for GWAS Catalog data source.

    This class is responsible for parsing additional curation for the GWAS Catalog studies.
    """

    @staticmethod
    def _parser(df: DataFrame) -> DataFrame:
        """Parse the curation table.

        Args:
            df (DataFrame): DataFrame with the curation table.

        Returns:
            DataFrame: DataFrame with the parsed curation table.
        """
        if "qualityControl" not in df.columns:
            # Add the 'qualityControl' column with null values
            df = df.withColumn("qualityControl", f.lit(None).cast("string"))
        return df.select(
            "studyId",
            "studyType",
            f.when(
                f.col("analysisFlag").isNotNull(), f.split(f.col("analysisFlag"), r"\|")
            )
            .otherwise(f.array())
            .alias("analysisFlags"),
            f.when(
                f.col("qualityControl").isNotNull(),
                f.split(f.col("qualityControl"), r"\|"),
            )
            .otherwise(f.array())
            .alias("qualityControls"),
            f.col("isCurated").cast(t.BooleanType()),
        )

    @classmethod
    def from_csv(
        cls: type[StudyIndexGWASCatalogOTCuration], session: Session, curation_path: str
    ) -> DataFrame:
        """Read curation table from csv.

        Args:
            session (Session): Session object.
            curation_path (str): Path to the curation table.

        Returns:
            DataFrame: DataFrame with the curation table.
        """
        return cls._parser(session.spark.read.csv(curation_path, sep="\t", header=True))

    @classmethod
    def from_url(
        cls: type[StudyIndexGWASCatalogOTCuration], session: Session, curation_url: str
    ) -> DataFrame:
        """Read curation table from URL.

        Args:
            session (Session): Session object.
            curation_url (str): URL to the curation table.

        Returns:
            DataFrame: DataFrame with the curation table.
        """
        # Registering file:
        session.spark.sparkContext.addFile(curation_url)

        return cls._parser(
            session.spark.read.csv(
                SparkFiles.get(curation_url.split("/")[-1]), sep="\t", header=True
            )
        )
