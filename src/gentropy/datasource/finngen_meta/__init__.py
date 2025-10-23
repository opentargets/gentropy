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


class FinnGenMetaManifest:
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
        |-- nCases: integer (nullable = true)
        |-- nSamplesPerCohort: array (nullable = true)
            |-- element: struct (containsNull = true)
                |-- cohort: string (nullable = true)
                |-- nSamples: integer (nullable = true)
        |-- nCasesPerCohort: array (nullable = true)
            |-- element: struct (containsNull = true)
                |-- cohort: string (nullable = true)
                |-- nCases: integer (nullable = true)
        |-- nControls: integer (nullable = true)
        |-- hasSumstats: boolean (nullable = true)
        |-- summarystatsLocation: string (nullable = true)  # may be null if not provided in the manifest
        ```
        """
        return self._df.select(
            self.study_id.alias("studyId"),
            self.project_id.alias("projectId"),
            self.trait_from_source.alias("traitFromSource"),
            self.discovery_samples.alias("discoverySamples"),
            self.n_samples.alias("nSamples"),
            self.n_samples_per_cohort.alias("nSamplesPerCohort"),
            self.n_cases.alias("nCases"),
            self.n_cases_per_cohort.alias("nCasesPerCohort"),
            self.n_controls.alias("nControls"),
            self.summary_statistics_location.alias("summarystatsLocation"),
            self.has_summary_statistics.alias("hasSumstats"),
        )

    @classmethod
    def from_path(cls, session: Session, manifest_path: str) -> FinnGenMetaManifest:
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
        assert cls.required_columns.issubset(set(df.columns)), (
            f"Manifest file must contain the following columns: {cls.required_columns}. "
        )

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

    @property
    def discovery_samples(self) -> Column:
        """Get the discovery samples.

        This method dispatches to the appropriate private method based on the meta-analysis data source.

        Returns:
            Column: Spark Column representing the discovery samples.
        """
        if self.meta == MetaAnalysisDataSource.FINNGEN_UKBB:
            return self._discovery_samples_finngen_ukbb()
        elif self.meta == MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
            return self._discovery_samples_finngen_ukbb_mvp()
        else:
            raise ValueError(f"Unsupported meta-analysis data source: {self.meta}")

    @staticmethod
    def _add(*cols: Column) -> Column:
        """Get the total number of samples from multiple columns.

        Args:
            *cols (Column): Columns to sum.

        Returns:
            Column: Column representing the total number of samples.


        Examples:
            >>> df = spark.createDataFrame([(1, 2, 3), (1, 2, None)], ["a", "b", "c"])
            >>> df.select(FinnGenMetaManifest._add(f.col("a"), f.col("b"), f.col("c")).alias("total")).show()
            +-----+
            |total|
            +-----+
            |    6|
            |    3|
            +-----+
            <BLANKLINE>
        """
        # Coalesce to 0 to handle nulls
        ccols = [f.coalesce(col, f.lit(0)) for col in cols]
        return reduce(operator.add, ccols).cast(t.IntegerType())

    @property
    def ancestry_columns(self) -> set[str]:
        """Find all ancestry number columns in the manifest.

        These columns are used to calculate the total number of samples, cases, and controls.

        Returns:
            set[str]: Set of ancestry number columns.

        Raises:
            ValueError: If the meta-analysis data source is unsupported.

        Examples:
            >>> dummy_df = spark.createDataFrame([(1,2,3), (4,5,6)])
            >>> manifest = FinnGenMetaManifest(df=dummy_df, meta=MetaAnalysisDataSource.FINNGEN_UKBB)
            >>> sorted(manifest.ancestry_columns)
            ['fg_n_cases', 'fg_n_controls', 'ukbb_n_cases', 'ukbb_n_controls']
            >>> manifest = FinnGenMetaManifest(df=dummy_df, meta=MetaAnalysisDataSource.FINNGEN_UKBB_MVP)
            >>> sorted(manifest.ancestry_columns)
            ['MVP_AFR_n_cases', 'MVP_AFR_n_controls', 'MVP_AMR_n_cases', 'MVP_AMR_n_controls', 'MVP_EUR_n_cases', 'MVP_EUR_n_controls', 'fg_n_cases', 'fg_n_controls', 'ukbb_n_cases', 'ukbb_n_controls']
        """
        if self.meta == MetaAnalysisDataSource.FINNGEN_UKBB:
            return self.finngen_ancestry_cols | self.ukbb_ancestry_cols
        elif self.meta == MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
            return (
                self.finngen_ancestry_cols
                | self.ukbb_ancestry_cols
                | self.mvp_ancestry_columns
            )
        else:
            raise ValueError(f"Unsupported meta-analysis data source: {self.meta}")

    @property
    def n_samples(self) -> Column:
        """Get the total number of samples."""
        return self._add(*[f.col(c) for c in self.ancestry_columns]).alias("nSamples")

    @property
    def n_cases(self) -> Column:
        """Get the total number of cases."""
        ancestry_cols = [f.col(c) for c in self.ancestry_columns if "n_cases" in c]
        return self._add(*ancestry_cols).alias("nCases")

    @property
    def n_controls(self) -> Column:
        """Get the total number of cases."""
        ancestry_cols = [f.col(c) for c in self.ancestry_columns if "n_controls" in c]
        return self._add(*ancestry_cols).alias("nControls")

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
                    (
                        f.coalesce(f.col("fg_n_cases"), f.lit(0))
                        + f.coalesce(f.col("fg_n_controls"), f.lit(0))
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("fin").alias("ancestry"),
                ),
                f.struct(
                    (
                        f.coalesce(f.col("ukbb_n_cases"), f.lit(0))
                        + f.coalesce(f.col("ukbb_n_controls"), f.lit(0))
                    )
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
                    (
                        f.coalesce(f.col("fg_n_cases"), f.lit(0))
                        + f.coalesce(f.col("fg_n_controls"), f.lit(0))
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("Finnish").alias("ancestry"),
                ),
                f.struct(
                    (
                        f.coalesce(f.col("ukbb_n_cases"), f.lit(0))
                        + f.coalesce(f.col("ukbb_n_controls"), f.lit(0))
                        + f.coalesce(f.col("MVP_EUR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_EUR_n_controls"), f.lit(0))
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("European").alias("ancestry"),
                ),
                f.struct(
                    (
                        f.coalesce(f.col("MVP_AFR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_AFR_n_controls"), f.lit(0))
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("African").alias("ancestry"),
                ),
                f.struct(
                    (
                        f.coalesce(f.col("MVP_AMR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_AMR_n_controls"), f.lit(0))
                    )
                    .cast(t.IntegerType())
                    .alias("sampleSize"),
                    f.lit("Admixed American").alias("ancestry"),
                ),
            ),
            lambda x: x.sampleSize > 0.0,
        ).alias("discoverySamples")

    @property
    def summary_statistics_location(self) -> Column:
        """Get the summary statistics location column.

        Returns:
            Column: Spark Column representing the summary statistics location.
        """
        if self.sumstat_location_column in self._df.columns:
            return (
                f.col(self.sumstat_location_column)
                .cast(t.StringType())
                .alias("summarystatsLocation")
            )
        else:
            return f.lit(None).cast(t.StringType()).alias("summarystatsLocation")

    @property
    def has_summary_statistics(self) -> Column:
        """Get the has summary statistics column.

        Returns:
            Column: Spark Column representing whether the study has summary statistics.
        """
        return f.lit(True).alias("hasSumstats")

    @property
    def study_id(self) -> Column:
        """Get the study ID column.

        Returns:
            Column: Spark Column representing the study ID.
        """
        return f.concat_ws(
            "_",
            f.lit(self.meta.value),
            f.col("fg_phenotype"),
        ).alias("studyId")

    @property
    def project_id(self) -> Column:
        """Get the project ID column.

        Returns:
            Column: Spark Column representing the project ID.
        """
        return f.lit(self.meta.value).alias("projectId")

    @property
    def trait_from_source(self) -> Column:
        """Get the trait from source column.

        Returns:
            Column: Spark Column representing the trait from source.
        """
        return f.col("name").alias("traitFromSource")

    @property
    def n_cases_per_cohort(self) -> Column:
        """Get the number of cases per cohort column.

        Returns:
            Column: Spark Column representing the number of cases per cohort.
        """
        n_cases = [
            f.struct(
                f.lit("FinnGen").alias("cohort"),
                f.coalesce(f.col("fg_n_cases"), f.lit(0)).alias("nCases"),
            ),
            f.struct(
                f.lit("UKBB").alias("cohort"),
                f.coalesce(f.col("ukbb_n_cases"), f.lit(0)).alias("nCases"),
            ),
        ]
        if self.meta == MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
            n_cases += [
                f.struct(
                    f.lit("MVP_EUR").alias("cohort"),
                    f.coalesce(f.col("MVP_EUR_n_cases"), f.lit(0)).alias("nCases"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    f.coalesce(f.col("MVP_AFR_n_cases"), f.lit(0)).alias("nCases"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    f.coalesce(f.col("MVP_AMR_n_cases"), f.lit(0)).alias("nCases"),
                ),
            ]

        return f.array(*n_cases).alias("nCasesPerCohort")

    @property
    def n_samples_per_cohort(self) -> Column:
        """Get the number of samples per cohort column.

        Returns:
            Column: Spark Column representing the number of samples per cohort.
        """
        n_samples = [
            f.struct(
                f.lit("FinnGen").alias("cohort"),
                (
                    f.coalesce(f.col("fg_n_cases"), f.lit(0))
                    + f.coalesce(f.col("fg_n_controls"), f.lit(0))
                ).alias("nSamples"),
            ),
            f.struct(
                f.lit("UKBB").alias("cohort"),
                (
                    f.coalesce(f.col("ukbb_n_cases"), f.lit(0))
                    + f.coalesce(f.col("ukbb_n_controls"), f.lit(0))
                ).alias("nSamples"),
            ),
        ]
        if self.meta == MetaAnalysisDataSource.FINNGEN_UKBB_MVP:
            n_samples += [
                f.struct(
                    f.lit("MVP_EUR").alias("cohort"),
                    (
                        f.coalesce(f.col("MVP_EUR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_EUR_n_controls"), f.lit(0))
                    ).alias("nSamples"),
                ),
                f.struct(
                    f.lit("MVP_AFR").alias("cohort"),
                    (
                        f.coalesce(f.col("MVP_AFR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_AFR_n_controls"), f.lit(0))
                    ).alias("nSamples"),
                ),
                f.struct(
                    f.lit("MVP_AMR").alias("cohort"),
                    (
                        f.coalesce(f.col("MVP_AMR_n_cases"), f.lit(0))
                        + f.coalesce(f.col("MVP_AMR_n_controls"), f.lit(0))
                    ).alias("nSamples"),
                ),
            ]

        return f.array(*n_samples).alias("nSamplesPerCohort")
