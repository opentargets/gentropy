"""deCODE proteomics datasource module."""

from __future__ import annotations

from enum import Enum

from pyspark.sql import DataFrame

from gentropy import Session, StudyIndex, SummaryStatistics

# 1. Build the studyIndex from the manifest
# 2. Download the raw deCODE data files
# 3. Download the smp deCODE data files
# 4. Convert to parquet
# 5. Harmonisation to standard format
# 6. QualityControls


class deCODEDataSource(Enum, str):
    """deCODE proteomics data sources."""

    DECODE_PROTEOMICS = "deCODE-proteomics"


class deCODEManifest:
    """deCODE manifest class."""

    def __init__(self, df: DataFrame) -> None:
        """Initialize deCODE manifest."""
        self._df = df

    @classmethod
    def from_path(cls, session: Session, path: str) -> deCODEManifest:
        """Create deCODE manifest from path.

        Args:
            session (Session): Gentropy session.
            path (str): Path to the manifest file.

        Returns:
            deCODEManifest: deCODE manifest instance.
        """
        # Implement logic to read and parse the manifest file from the given path

        manifest_df = (
            session.spark.read.format("csv")
            .option("header", "true")
            .option("sep", "\t")
            .load(path)
        )
        return cls(df=manifest_df)


class deCODEStudyIndex:
    """deCODE study index class."""

    @classmethod
    def from_manifest(cls, manifest: deCODEManifest) -> StudyIndex:
        """Create deCODE study index from manifest.

        Args:
            manifest (deCODEManifest): deCODE manifest.

        Returns:
            deCODEStudyIndex: deCODE study index instance.
        """
        raise NotImplementedError("deCODE study index creation not implemented yet.")


class deCODESummaryStatistics:
    """deCODE summary statistics class."""

    @classmethod
    def from_study_index(cls, study_index: StudyIndex) -> SummaryStatistics:
        """Create deCODE summary statistics from study index.

        Args:
            study_index (StudyIndex): deCODE study index.

        Returns:
            SummaryStatistics: deCODE summary statistics instance.
        """
        raise NotImplementedError(
            "deCODE summary statistics creation not implemented yet."
        )

    @staticmethod
    def tsv_to_parquet(session, summary_statistics_paths: list[str], raw_summary_statistics_path: str,)
