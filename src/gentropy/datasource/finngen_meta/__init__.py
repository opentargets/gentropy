"""Finngen meta analysis data source module."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from gentropy import Session

import operator
from enum import Enum
from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql import types as t


class MetaAnalysisDataSource(str, Enum):
    """Enum for meta-analysis data sources."""

    FINNGEN_UKBB_MVP = "FINNGEN_R12_UKB_MVP_META"
    FINNGEN_UKBB = "FINNGEN_R12_UKB_META"


class FinngenMetaManifest:
    """FinnGen meta-analysis manifest."""

    ukbb_ancestry_cols = {
        "ukbb_n_cases",
        "ukbb_n_controls",
    }

    finngen_ancestry_cols = {
        "fg_n_cases",
        "fg_n_controls",
    }

    required_columns = {
        "fg_phenotype",  # Original Finngen studyId (e.g. "I9_HEARTFAIL")
        "name",  # Finngen phenotype name - used for mapping to EFO
        # Ancestry columns for finngen and UKBB (should be in both meta analyses)
        *finngen_ancestry_cols,
        *ukbb_ancestry_cols,
    }

    mvp_ancestry_columns = {
        "MVP_AFR_n_cases",
        "MVP_AFR_n_controls",
        "MVP_EUR_n_cases",
        "MVP_EUR_n_controls",
        "MVP_AMR_n_cases",
        "MVP_AMR_n_controls",
    }
    sumstat_location_column = "path_bucket"

    def __init__(self, df: DataFrame, meta: MetaAnalysisDataSource) -> None:
        """Initialize the FinnGen meta-analysis manifest.

        Args:
            df (DataFrame): DataFrame containing the manifest data.
            meta (MetaAnalysisDataSource): Meta-analysis data source enum.
        """
        self.meta = meta
        self._df = df

    @property
    def df(self) -> DataFrame:
        """Get the manifest DataFrame.

        The resulting DataFrame has the following schema:
        ```
        |-- studyPhenotype: string (nullable = true)
        |-- traitFromSource: string (nullable = true)
        |-- discoverySamples: array (nullable = true)
            |-- element: struct (containsNull = true)
                |-- sampleSize: integer (nullable = true)
                |-- ancestry: string (nullable = true)
        |-- nSamples: integer (nullable = true)
        |-- hasSumstats: boolean (nullable = true)
        |-- summarystatsLocation: string (nullable = true)  # may be null if not provided in the manifest
        ```
        """
        return self._df.select(
            f.col("fg_phenotype").alias("studyPhenotype"),
            f.col("name").alias("traitFromSource"),
            self.discovery_samples().alias("discoverySamples"),
            self.n_samples().alias("nSamples"),
            f.lit(True).alias("hasSumstats"),
            f.col("path_bucket").alias("summarystatsLocation"),
        )

    @classmethod
    def from_path(cls, session: Session, manifest_path: str) -> FinngenMetaManifest:
        """Load the FinnGen meta-analysis manifest from a specified path.

        Note:
            This method asserts that the manifest file is tab-delimited and contains header with following columns:
            ```
            |-- fg_phenotype: string (nullable = true)        # required
            |-- name: string (nullable = true)                # required
            |-- fg_n_cases: integer (nullable = true)         # required
            |-- fg_n_controls: integer (nullable = true)      # required
            |-- ukbb_n_cases: integer (nullable = true)       # required
            |-- ukbb_n_controls: integer (nullable = true)    # required
            |-- MVP_AFR_n_cases: integer (nullable = true)    # optional
            |-- MVP_AFR_n_controls: integer (nullable = true) # optional
            |-- MVP_EUR_n_cases: integer (nullable = true)    # optional
            |-- MVP_EUR_n_controls: integer (nullable = true) # optional
            |-- MVP_AMR_n_cases: integer (nullable = true)    # optional
            |-- MVP_AMR_n_controls: integer (nullable = true) # optional
            |-- path_bucket: string (nullable = true)         # optional
            ```
        Args:
            session (Session): Session object.
            manifest_path (str): Path to the manifest file.

        Returns:
            FinngenMetaManifest: Loaded manifest object.

        Raises:
            AssertionError: If the manifest file does not contain the required columns.
            AssertionError: If the manifest file does not contain the required columns.
        """
        df = (
            session.spark.read.option("header", True)
            .option("sep", "\t")
            .csv(manifest_path)
        )
        assert cls.required_columns.issubset(
            set(df.columns)
        ), f"Manifest file must contain the following columns: {cls.required_columns}. "

        # By default we assume we are dealing with the FinnGen UKBB meta-analysis
        meta = MetaAnalysisDataSource.FINNGEN_UKBB
        columns = [*cls.required_columns]

        # If we have the MVP ancestry columns, then we are dealing with the FinnGen UKBB MVP meta-analysis
        if cls.mvp_ancestry_columns.issubset(set(df.columns)):
            meta = MetaAnalysisDataSource.FINNGEN_UKBB_MVP
            columns += [*cls.mvp_ancestry_columns]

        column_map = [
            f.col(col).cast(t.IntegerType()).alias(col)
            if "n_cases" in col or "n_controls" in col
            else f.col(col).cast(t.StringType()).alias(col)
            for col in columns
        ]

        # Handle the summary statistics location.
        if cls.sumstat_location_column in df.columns:
            column_map.append(
                f.col(cls.sumstat_location_column)
                .cast(t.StringType())
                .alias(cls.sumstat_location_column)
            )
        else:
            session.logger.warning(
                f"Manifest file does not contain the '{cls.sumstat_location_column}' column. Can not determine summary statistics location."
            )
            column_map.append(f.lit(None).alias(cls.sumstat_location_column))

        df = df.select(*column_map)  # Final contract
        return cls(df=df, meta=meta)

    def discovery_samples(self) -> Column:
        """Get the discovery samples.

        This method dispatches to the appropriate private method based on the meta-analysis data source.

        Returns:
            Column: Spark Column representing the discovery samples.
        """
        match self.meta:
            case MetaAnalysisDataSource.FINNGEN_UKBB:
                return self._discovery_samples_finngen_ukbb()
            case MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
                return self._discovery_samples_finngen_ukbb_mvp()
            case _:
                raise ValueError(f"Unsupported meta-analysis data source: {self.meta}")

    @staticmethod
    def _add(*cols: Column) -> Column:
        """Get the total number of samples from multiple columns.

        Args:
            *cols (Column): Columns to sum.

        Returns:
            Column: Column representing the total number of samples.
        """
        return reduce(operator.add, cols).cast(t.IntegerType()).alias("nSamples")

    def n_samples(self) -> Column:
        """Get the total number of samples."""
        match self.meta:
            case MetaAnalysisDataSource.FINNGEN_UKBB:
                ancestry_cols = [
                    f.col(c)
                    for c in self.finngen_ancestry_cols.union(self.ukbb_ancestry_cols)
                ]
            case MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
                ancestry_cols = [
                    f.col(c)
                    for c in self.finngen_ancestry_cols.union(
                        self.ukbb_ancestry_cols.union(self.mvp_ancestry_columns)
                    )
                ]
            case _:
                raise ValueError(f"Unsupported meta-analysis data source: {self.meta}")
        return self._add(*ancestry_cols).alias("nSamples")

    def _discovery_samples_finngen_ukbb(self) -> Column:
        """Get the discovery samples for FinnGen UKBB meta-analysis.

        This meta analysis includes only two cohorts:
        - Finnish (from FinnGen)
        - Non-Finnish European (from Pan-UKBB European subset)

        All ancestries with sample size > 0 are included.

        Returns:
            Column: Spark Column representing the ancestry cocktail.
        """
        return f.filter(
            f.array(
                f.struct(
                    (f.col("fg_n_cases") + f.col("fg_n_controls"))
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("fin").alias("ancestry"),
                ),
                f.struct(
                    (f.col("ukbb_n_cases") + f.col("ukbb_n_controls"))
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("nfe").alias("ancestry"),
                ),
            ),
            lambda x: x.sampleSize > 0.0,
        ).alias("discoverySamples")

    def _discovery_samples_finngen_ukbb_mvp(self) -> Column:
        """Get the discovery samples for FinnGen UKBB MVP meta-analysis.

        This meta analysis includes n of four cohorts:
        - Finnish (from FinnGen)
        - European (from Pan-UKBB European subset and MVP European subset)
        - African (from MVP African subset)
        - American (from MVP American subset)

        All ancestries with sample size > 0 are included.

        Returns:
            Column: Spark Column representing the ancestry cocktail.
        """
        return f.filter(
            f.array(
                f.struct(
                    (f.col("fg_n_cases") + f.col("fg_n_controls"))
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("Finninsh").alias("ancestry"),
                ),
                f.struct(
                    (
                        f.col("ukbb_n_cases")
                        + f.col("ukbb_n_controls")
                        + f.col("MVP_EUR_n_cases")
                        + f.col("MVP_EUR_n_controls")
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("European").alias("ancestry"),
                ),
                f.struct(
                    (f.col("MVP_AFR_n_cases") + f.col("MVP_AFR_n_controls"))
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("afr").alias("ancestry"),
                ),
                f.struct(
                    (f.col("MVP_AMR_n_cases") + f.col("MVP_AMR_n_controls"))
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("amr").alias("ancestry"),
                ),
            ),
            lambda x: x.sampleSize > 0.0,
        ).alias("discoverySamples")


class EFOCuration:
    """EFO curation for FinnGen meta-analysis."""

    required_columns = {"STUDY", "PROPERTY_VALUE", "SEMANTIC_TAG"}

    def __init__(self, df: DataFrame) -> None:
        """Initialize the EFO curation.

        Args:
            df (DataFrame): DataFrame containing the EFO curation data.
        """
        self.df = df

    @classmethod
    def from_path(cls, session: Session, efo_curation_path: str) -> EFOCuration:
        """Load the EFO curation from a specified path.

        Note:
            This method asserts that the EFO curation file is tab-delimited and contains header with following columns:
            ```
            |-- STUDY: string (nullable = true)           # required
            |-- PROPERTY_VALUE: string (nullable = true)  # required
            |-- SEMANTIC_TAG: string (nullable = true)    # required
            ```
        Args:
            session (Session): Session object.
            efo_curation_path (str): Path to the EFO curation file.

        Returns:
            EFOCuration: Loaded EFO curation object.

        Raises:
            AssertionError: If the EFO curation file does not contain the required columns.

        """
        if efo_curation_path.startswith("http"):
            from pyspark import SparkFiles

            session.spark.sparkContext.addFile(efo_curation_path)
            efo_curation_path = "file://" + SparkFiles.get(
                efo_curation_path.split("/")[-1]
            )

        efo_curation_mapping = session.spark.read.csv(
            efo_curation_path,
            sep="\t",
            header=True,
        )
        assert cls.required_columns.issubset(
            set(efo_curation_mapping.columns)
        ), f"EFO curation file must contain the following columns: {cls.required_columns}."
        columns = [
            f.col(col).cast(t.StringType()).alias(col) for col in cls.required_columns
        ]
        df = efo_curation_mapping.select(*columns)
        return cls(df=df)
