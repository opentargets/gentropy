"""StudyIndex class for deCODE proteomics data source."""

from __future__ import annotations

from functools import partial
from typing import NamedTuple

from pyspark.sql import functions as f
from pyspark.sql.column import Column

from gentropy import StudyIndex, TargetIndex
from gentropy.datasource.decode import deCODEDataSource, deCODEManifest


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
        return r"^([\w-_]+?)_([\w-]+)_(\d+_\d+)_([A-Za-z0-9]+)_(\w+)_\d+$"

    @classmethod
    def extract_gene_symbol(cls, study_id: Column) -> deCODEStudyIdParts:
        """Extract gene symbol from study ID.

        Args:
            study_id (Column): Study ID column.

        Returns:
            deCODEStudyIdParts: Extracted parts of the study ID.
        """
        p = partial(f.regexp_extract, study_id, cls.get_parttern())
        return cls(
            project_id=p(1).alias("projectId"),
            datasource_type=p(2).alias("datasourceType"),
            aptamer_id=p(3).alias("aptamerId"),
            gene_symbol=p(4).alias("geneSymbol"),
            protein_name=p(5).alias("proteinName"),
        )


class deCODEPublicationMetadata(NamedTuple):
    """deCODE publication metadata."""

    PUBMED_ID = "37794188"
    """PubMed ID for the deCODE proteomics study."""
    PUB_TITLE = "Large-scale plasma proteomics comparisons through genetics and disease associations"
    """Title of the deCODE proteomics publication."""
    PUB_FIRST_AUTHOR = "Eldjarn GH, Ferkingstad E"
    """First author(s) of the deCODE proteomics publication."""
    PUB_DATE = "2024"
    """Publication date of the deCODE proteomics study."""
    PUB_JOURNAL = "Nature"
    """Journal where the deCODE proteomics study was published."""
    SMP_SAMPLE_SIZE = 35892
    """Sample size for SMP-normalized proteomics data."""
    SAMPLE_SIZE = 36136
    """Sample size for non-normalized proteomics data."""
    ANCESTRY = "Icelandic"
    """Ancestry of the study population."""
    COHORTS = "deCODE"
    """Cohorts involved in the deCODE proteomics study."""
    BIOSAMPLE_ID = "UBERON_0001969"
    """Biosample ID for deCODE proteomics study - blood plasma."""

    @classmethod
    def get_initial_sample(cls, project_id: Column) -> Column:
        """Get initial sample size based on projectId.

        Args:
            project_id (Column): Project ID column.

        Returns:
            Column: Initial sample size column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.lit(f"{cls.SAMPLE_SIZE:,} Icelandic individuals"),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.lit(f"{cls.SMP_SAMPLE_SIZE:,} Icelandic individuals"),
            )
            .otherwise(f.lit(None))
            .alias("initialSampleSize")
        )

    @classmethod
    def get_n_samples(cls, project_id: Column) -> Column:
        """Get number of samples based on projectId.

        Args:
            project_id (Column): Project ID column.

        Returns:
            Column: Number of samples column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.lit(cls.SAMPLE_SIZE),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.lit(cls.SMP_SAMPLE_SIZE),
            )
            .otherwise(f.lit(None))
            .alias("nSamples")
        )

    @classmethod
    def get_discovry_samples(cls, project_id: Column) -> Column:
        """Get discoverySamples based on projectId.

        Args:
            project_id (Column): Project ID column.

        Returns:
            Column: Number of discovery samples column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.array(
                    f.struct(
                        f.lit(cls.SAMPLE_SIZE).alias("sampleSize"),
                        f.lit(cls.ANCESTRY).alias("ancestry"),
                    )
                ),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.array(
                    f.struct(
                        f.lit(cls.SMP_SAMPLE_SIZE).alias("sampleSize"),
                        f.lit(cls.ANCESTRY).alias("ancestry"),
                    )
                ),
            )
            .otherwise(f.lit(None))
            .alias("nDiscoverySamples")
        )


class deCODEStudyIndex:
    """deCODE study index class."""

    @classmethod
    def from_manifest(
        cls,
        manifest: deCODEManifest,
        target_index: TargetIndex,
    ) -> StudyIndex:
        """Create deCODE study index from manifest.

        Args:
            manifest (deCODEManifest): deCODE manifest.
            target_index (TargetIndex): Target index dataset.

        Returns:
            StudyIndex: deCODE study index instance.
        """
        _ti_lut = f.broadcast(
            target_index.symbols_lut().select("geneSymbol", "geneId")
        ).alias("ti_lut")

        id_split = deCODEStudyIdParts.extract_gene_symbol(f.col("studyId"))
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
        pub = deCODEPublicationMetadata
        return StudyIndex(
            _df=manifest_w_gid.withColumn("studyType", f.lit("pqtl"))
            .withColumn("biosampleFromSourceId", f.lit(pub.BIOSAMPLE_ID))
            .withColumn("pubmedId", f.lit(pub.PUBMED_ID))
            .withColumn("publicationFirstAuthor", f.lit(pub.PUB_FIRST_AUTHOR))
            .withColumn("publicationDate", f.lit(pub.PUB_DATE))
            .withColumn("publicationJournal", f.lit(pub.PUB_JOURNAL))
            .withColumn("publicationTitle", f.lit(pub.PUB_TITLE))
            .withColumn("initialSampleSize", pub.get_initial_sample(f.col("projectId")))
            .withColumn("nSamples", pub.get_n_samples(f.col("projectId")))
            .withColumn(
                "discoverySamples", pub.get_discovry_samples(f.col("projectId"))
            )
            .withColumn(
                "ldPopulationStructure",
                StudyIndex.aggregate_and_map_ancestries(f.col("discoverySamples")),
            )
            .withColumn("cohorts", f.array(f.lit(pub.COHORTS)))
            .select(
                "studyId",
                "geneId",
                "projectId",
                "studyType",
                "traitFromSource",
                # f.lit(None).cast("array<string>").alias("traitFromSourceMappedIds"),
                "biosampleFromSourceId",
                "pubmedId",
                "publicationTitle",
                "publicationFirstAuthor",
                "publicationDate",
                "publicationJournal",
                # f.lit(None)
                # .cast("array<string>")
                # .alias("backgroundTraitsFromSourceMappedIds"),
                "initialSampleSize",
                # f.lit(None).cast(t.IntegerType()).alias("nCases"),
                # f.lit(None).cast(t.IntegerType()).alias("nControls"),
                "nSamples",
                "cohorts",
                "ldPopulationStructure",
                "discoverySamples",
                # f.lit(None)
                # .cast("array<struct<sampleSize:int,ancestry:string>>")
                # .alias("replicationSamples"),
                # f.lit(None).cast("array<string>").alias("qualityControls"),
                # f.lit(None).cast("array<string>").alias("aalysisFlags"),
                "summarystatsLocation",
                "hasSumstats",
                # f.lit(None).cast("string").alias("conditions"),
                # f.array()
                # .cast("array<struct<QCCheckName:string,QCCheckValue:float>>")
                # .alias("sumstatQCValues"),
                # f.lit(None).cast("array<string>").alias("diseaseIds"),
                # f.lit(None).cast("array<string>").alias("backgroundDiseaseIds"),
            )
        )
