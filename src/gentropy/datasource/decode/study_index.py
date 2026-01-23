"""StudyIndex class for deCODE proteomics data source."""

from __future__ import annotations

from pyspark.sql import functions as f

from gentropy import StudyIndex, TargetIndex
from gentropy.datasource.decode import deCODEManifest
from pyspark.sql.column import Column
from typing import NamedTuple
from functools import partial


class deCODEStudyIdParts(NamedTuple):
    """deCODE study ID parts."""

    project_id: Column
    """Project ID extracted from the study ID."""
    datasource_type: Column
    """Data source type - SMP or Raw."""
    aptamer_id: Column
    """Aptamer identifier extracted from the study ID."""
    gene_symbol: Column
    """Gene Symbol derived from the study ID."""
    protein_name: Column
    """Protein name derived from the study ID."""

    @property
    def trait(self) -> Column:
        """Trait derived from the study ID."""
        return f.concat_ws(
            "_",
            self.datasource_type,
            self.aptamer_id,
            self.gene_symbol,
            self.protein_name,
        ).alias("traitFromSource")

    @staticmethod
    def get_parttern() -> str:
        """Regex pattern to extract deCODE study ID parts.

        Returns:
            str: Regex pattern.

        The pattern captures the following groups:
        (1) project ID
        (2) study type
        (3) aptamer identifier
        (4) gene symbol
        (5) protein name
        (2-5) trait
        """
        return r"^([\w-]+?)_(([\w-]+)_(\d+_\d+)_([A-Za-z0-9]+)_(\w+))_\d+$"


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
        _ti_lut = f.broadcast(
            target_index.symbols_lut().select(
                f.col("geneSymbol"), f.col("id").alias("geneId")
            )
        ).alias("ti_lut")

        id_split = cls._extract_gene_symbol(f.col("studyId"))
        _manifest = manifest.df.select(
            f.col("projectId"),
            f.col("studyId"),
            f.col("hasSumstats"),
            f.col("summarystatsLocation"),
            id_split.gene_symbol.alias("geneSymbol"),
            id_split.trait.alias("traitFromSource"),
        ).alias("manifest")

        # Preserve all rows from the manifest
        manifest_w_gid = _manifest.join(_ti_lut, on="geneSymbol", how="left").persist()
        _ti_lut.unpersist()
        return (
            manifest_w_gid.withColumn(
                "qualityControls", f.array().cast("array<string>")
            )
            .withColumn("studyType", f.lit("pqtl"))
            .withColumn(
                "traitFromSource",
            )
        )

        df = (
            manifest.df.join()
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

    @staticmethod
    def _extract_gene_symbol(study_id: Column) -> deCODEStudyIdParts:
        """Extract gene symbol from study ID.

        Args:
            study_id (Column): Study ID column.

        Returns:
            deCODEStudyIdParts: Extracted parts of the study ID.
        """
        p = partial(f.regexp_extract, study_id, deCODEStudyIdParts.get_parttern())
        return deCODEStudyIdParts(
            project_id=p(0).alias("projectId"),
            datasource_type=p(1).alias("datasourceType"),
            aptamer_id=p(2).alias("aptamerId"),
            gene_symbol=p(3).alias("geneSymbol"),
            protein_name=p(4).alias("proteinName"),
        )
