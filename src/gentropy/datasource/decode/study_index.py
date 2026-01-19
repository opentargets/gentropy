"""StudyIndex class for deCODE proteomics data source."""

from __future__ import annotations
from pyspark.sql import functions as f

from gentropy import StudyIndex, TargetIndex
from gentropy.datasource.decode import deCODEManifest


class deCODEStudyIndex:
    """deCODE study index class."""

    @classmethod
    def from_manifest(
        cls,
        manifest: deCODEManifest,
        target_index: TargetIndex,
        raw_summary_statistics_path: str,
    ) -> StudyIndex:
        """Create deCODE study index from manifest.

        Args:
            manifest (deCODEManifest): deCODE manifest.

        Returns:
            deCODEStudyIndex: deCODE study index instance.
        """
        df = (
            manifest._df.withColumn("geneId", gene_id)
            .withColumn("studyType", f.lit("pqtl"))
            .withColumn(
                "traitFromSource", f.regexp_extract("studyId", r"Proteomics_(.*)", 1)
            )
            .withColumn("biosampleFromSourceId", f.lit("UBERON_0001969"))
        ).select(
            "projectId",
            "studyId",
            "studyType",
            "traitFromSource",
            "geneId",
            "hasSumstats",
            "summarystatsLocation",
            "biosampleFromSourceId",
        )
        # Get the nSamples from the summaryStatistics

        return StudyIndex(_df=manifest._df)
