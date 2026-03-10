"""deCODE pQTL study index module.

This module provides two main classes:

- **`deCODEStudyIdParts`** – a `NamedTuple` of Spark `Column` objects that represent
  the structured components of a deCODE study ID (project, assay type, aptamer,
  gene symbol, protein name).

- **`deCODEStudyIndex`** – static factory helpers that build a
  `ProteinQuantitativeTraitLocusStudyIndex`
  from the deCODE manifest, aptamer metadata, and molecular-complex datasets.

The study ID format used throughout the pipeline is::

    {projectId}_{datasourceType}_{aptamerId}_{geneSymbol}_{proteinName}_{sequenceNumber}

For example::

    deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001
"""

from __future__ import annotations

from functools import partial
from typing import NamedTuple

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.column import Column

from gentropy import StudyIndex
from gentropy.dataset.molecular_complex import MolecularComplex
from gentropy.dataset.study_index import ProteinQuantitativeTraitLocusStudyIndex
from gentropy.datasource.decode import deCODEDataSource, deCODEPublicationMetadata
from gentropy.datasource.decode.aptamer_metadata import AptamerMetadata
from gentropy.datasource.decode.manifest import deCODEManifest


class deCODEStudyIdParts(NamedTuple):
    """deCODE study ID parts.

    Examples:
        >>> data = [("deCODE-proteomics-smp_Proteomics_SMP_PC0_10000_2_GENE1_PROTEIN1_00000001",),]
        >>> schema = "studyId STRING"
        >>> df = spark.createDataFrame(data, schema)
        >>> id_parts = deCODEStudyIdParts.extract_study_id_parts(f.col("studyId"))
        >>> df.select(*id_parts).show(truncate=False)
        +---------------------+------------------+---------+---------------------+----------------------+
        |projectId            |datasourceType    |aptamerId|geneSymbolFromStudyId|proteinNameFromStudyId|
        +---------------------+------------------+---------+---------------------+----------------------+
        |deCODE-proteomics-smp|Proteomics_SMP_PC0|10000-2  |GENE1                |PROTEIN1              |
        +---------------------+------------------+---------+---------------------+----------------------+
        <BLANKLINE>

        >>> df.select(id_parts.trait).show(truncate=False)
        +-----------------------------------------+
        |traitFromSource                          |
        +-----------------------------------------+
        |Proteomics_SMP_PC0_10000-2_GENE1_PROTEIN1|
        +-----------------------------------------+
        <BLANKLINE>

    """

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
    def get_pattern() -> str:
        """Regex pattern to extract deCODE study ID parts.

        Returns:
            str: Regex pattern.

        The pattern captures the following groups:
        (1) project ID
        (2) study type
        (3) inner part (PC0 or SMP_PC0)
        (4) aptamer identifier
        (5) gene symbol
        (6) protein name
        (2-6) trait
        """
        return r"^([\w-_]+?)_(Proteomics_(SMP_PC0|PC0))_(\d+_\d+)_([A-Za-z0-9]+)_(\w+)_\d+$"

    @classmethod
    def extract_study_id_parts(cls, study_id: Column) -> deCODEStudyIdParts:
        """Extract gene symbol from study ID.

        Args:
            study_id (Column): Study ID column.

        Returns:
            deCODEStudyIdParts: Extracted parts of the study ID.
        """
        p = partial(f.regexp_extract, study_id, cls.get_pattern())
        return cls(
            project_id=p(1).alias("projectId"),
            datasource_type=p(2).alias("datasourceType"),
            aptamer_id=f.regexp_replace(p(4), "_", "-").alias("aptamerId"),
            gene_symbol=cls._mark_missing_gene_id(p(5)).alias("geneSymbolFromStudyId"),
            protein_name=cls._mark_missing_protein(p(6)).alias(
                "proteinNameFromStudyId"
            ),
        )

    @staticmethod
    def _mark_missing_gene_id(gene_id: Column) -> Column:
        """Mark the geneId as null if `NA` is in geneId.

        Args:
            gene_id (Column): Gene ID column extracted from the study ID.

        Returns:
            Column: Gene ID column with ``NA`` values replaced by ``null``.
        """
        return f.when(
            gene_id == f.lit("NA"),
            f.lit(None).cast(t.StringType()),
        ).otherwise(gene_id)

    @staticmethod
    def _mark_missing_protein(protein_name: Column) -> Column:
        """Mark the protein name as null if it is a placeholder value.

        Args:
            protein_name (Column): Protein name column extracted from the study ID.

        Returns:
            Column: Protein name column with ``Deprecated`` and ``No_Protein``
                values replaced by ``null``.
        """
        return f.when(
            (
                (protein_name == f.lit("Deprecated"))
                | (protein_name == f.lit("No_Protein"))
            ),
            f.lit(None).cast(t.StringType()),
        ).otherwise(protein_name)


class deCODEStudyIndex:
    """Factory helpers for constructing the deCODE pQTL study index.

    This class is not instantiated directly. Its class methods transform the
    `deCODEManifest`,
    `AptamerMetadata`, and
    `MolecularComplex` datasets into a
    `ProteinQuantitativeTraitLocusStudyIndex`.

    Sample-size and ancestry metadata are populated from
    `deCODEPublicationMetadata`, and the
    study ID is optionally updated after harmonisation to incorporate curated
    gene symbols and protein names sourced from the aptamer mapping table.
    """

    @classmethod
    def get_initial_sample(
        cls, project_id: Column, metadata: deCODEPublicationMetadata
    ) -> Column:
        """Get initial sample size based on projectId.

        Args:
            project_id (Column): Project ID column.
            metadata (deCODEPublicationMetadata): Metadata for the deCODE publication.

        Returns:
            Column: Initial sample size column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.lit(f"{metadata.SAMPLE_SIZE:,} Icelandic individuals"),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.lit(f"{metadata.SMP_SAMPLE_SIZE:,} Icelandic individuals"),
            )
            .otherwise(f.lit(None))
            .alias("initialSampleSize")
        )

    @classmethod
    def get_n_samples(
        cls, project_id: Column, metadata: deCODEPublicationMetadata
    ) -> Column:
        """Get number of samples based on projectId.

        Args:
            project_id (Column): Project ID column.
            metadata (deCODEPublicationMetadata): Metadata for the deCODE publication.

        Returns:
            Column: Number of samples column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.lit(metadata.SAMPLE_SIZE),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.lit(metadata.SMP_SAMPLE_SIZE),
            )
            .otherwise(f.lit(None))
            .alias("nSamples")
        )

    @classmethod
    def get_discovery_samples(
        cls, project_id: Column, metadata: deCODEPublicationMetadata
    ) -> Column:
        """Get discoverySamples based on projectId.

        Args:
            project_id (Column): Project ID column.
            metadata (deCODEPublicationMetadata): Metadata for the deCODE publication.

        Returns:
            Column: Number of discovery samples column.
        """
        return (
            f.when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_RAW.value),
                f.array(
                    f.struct(
                        f.lit(metadata.SAMPLE_SIZE).alias("sampleSize"),
                        f.lit(metadata.ANCESTRY).alias("ancestry"),
                    )
                ),
            )
            .when(
                project_id == f.lit(deCODEDataSource.DECODE_PROTEOMICS_SMP.value),
                f.array(
                    f.struct(
                        f.lit(metadata.SMP_SAMPLE_SIZE).alias("sampleSize"),
                        f.lit(metadata.ANCESTRY).alias("ancestry"),
                    )
                ),
            )
            .otherwise(f.lit(None))
            .alias("nDiscoverySamples")
        )

    @classmethod
    def from_manifest(
        cls,
        manifest: deCODEManifest,
        aptamer_metadata: AptamerMetadata,
        molecular_complex: MolecularComplex,
    ) -> ProteinQuantitativeTraitLocusStudyIndex:
        """Build a pQTL study index by joining the manifest, aptamer table, and protein complexes.

        Processing steps:

        1. Parse the structured study-ID components (project, assay type, aptamer ID,
           gene symbol, protein name) from every row in the manifest.
        2. Filter out rows where the gene symbol or protein name encoded in the study ID
           is missing (``NA`` / ``No_Protein`` / ``Deprecated``).
        3. Inner-join the aptamer mapping table on ``aptamerId`` to obtain curated target
           metadata; this restricts the study index to aptamers present in the SomaScan
           study table (a subset of the full manifest).
        4. Left-join the protein-complex table on the sorted, comma-joined UniProt protein
           ID string to annotate multi-target aptamers with a ``molecularComplexId``.
        5. Populate bibliographic and cohort metadata from
           `deCODEPublicationMetadata`, including
           sample sizes, biosample ID, ancestry, and LD population structure.

        Args:
            manifest (deCODEManifest): Manifest dataset cataloguing all available assays.
            aptamer_metadata (AptamerMetadata): Curated aptamer-to-protein mapping table.
            molecular_complex (MolecularComplex): Protein complex annotations from
                `ComplexTab`.

        Returns:
            ProteinQuantitativeTraitLocusStudyIndex: Study index ready for harmonisation.
        """
        id_split = deCODEStudyIdParts.extract_study_id_parts(f.col("studyId"))
        _manifest = (
            manifest.df.select(
                f.col("projectId"),
                f.col("studyId"),
                f.col("hasSumstats"),
                f.col("summarystatsLocation"),
                id_split.trait.alias("traitFromSource"),
                id_split.aptamer_id.alias("aptamerId"),
                id_split.datasource_type.alias("datasourceType"),
                id_split.gene_symbol.alias("geneSymbolFromStudyId"),
                id_split.gene_symbol.alias("geneFromSourceSymbol"),
                id_split.protein_name.alias("proteinNameFromSource"),
            )
            # Drop when the gene symbol or protein name are missing
            .filter(f.col("geneFromSourceSymbol").isNotNull())
            .filter(f.col("proteinNameFromSource").isNotNull())
            .drop("geneFromSourceSymbol", "proteinNameFromSource")
            .alias("_manifest")
        )

        _aptamers = aptamer_metadata.df.select(
            f.col("aptamerId").alias("aptamerId"),
            f.col("targetMetadata").alias("targetMetadata"),
        ).alias("_aptamers")

        # StudyId from manifest contains the molecular trait information,
        # but the mapping is inaccurate, so we decide to limit the number of assays
        # to trait metadata from the aptamer mapping table which is a subset of the manifest.
        _manifest_aptamer_annotated = (
            _manifest.join(_aptamers, on="aptamerId", how="inner")
            .withColumn(
                "proteinIds",
                f.concat_ws(
                    ",",
                    f.array_sort(
                        f.array_distinct(
                            f.transform(
                                "targetMetadata", lambda x: x.getField("proteinId")
                            )
                        )
                    ),
                ),
            )
            .alias("_manifest_aptamer_annotated")
        )

        _protein_complex = molecular_complex.df.select(
            f.col("id").alias("molecularComplexId"),
            f.concat_ws(
                ",",
                f.array_sort(
                    f.array_distinct(
                        f.transform(f.col("components"), lambda x: x.getField("id"))
                    )
                ),
            ).alias("proteinIds"),
        ).alias("_protein_complex")

        _manifest_complex_annotated = (
            _manifest_aptamer_annotated.join(
                _protein_complex, on="proteinIds", how="left"
            )
            .drop("proteinIds")
            .withColumn("geneId", f.lit(None).cast(t.StringType()))
            .withColumn(
                "targetsFromSource",
                f.transform(
                    "targetMetadata",
                    lambda x: x.withField(
                        "geneId", f.lit(None).cast(t.StringType())
                    ).withField("proteinName", f.lit(None).cast(t.StringType())),
                ),
            )
        )

        # Preserve all rows from the manifest
        pub = deCODEPublicationMetadata()
        return ProteinQuantitativeTraitLocusStudyIndex(
            _df=_manifest_complex_annotated.withColumn("studyType", f.lit("pqtl"))
            .withColumn("biosampleFromSourceId", f.lit(pub.BIOSAMPLE_ID))
            .withColumn("pubmedId", f.lit(pub.PUBMED_ID))
            .withColumn("publicationFirstAuthor", f.lit(pub.PUB_FIRST_AUTHOR))
            .withColumn("publicationDate", f.lit(pub.PUB_DATE))
            .withColumn("publicationJournal", f.lit(pub.PUB_JOURNAL))
            .withColumn("publicationTitle", f.lit(pub.PUB_TITLE))
            .withColumn(
                "initialSampleSize", cls.get_initial_sample(f.col("projectId"), pub)
            )
            .withColumn("nSamples", cls.get_n_samples(f.col("projectId"), pub))
            .withColumn(
                "discoverySamples", cls.get_discovery_samples(f.col("projectId"), pub)
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
                f.lit(None).cast("array<string>").alias("qualityControls"),
                # f.lit(None).cast("array<string>").alias("aalysisFlags"),
                "summarystatsLocation",
                "hasSumstats",
                # f.lit(None).cast("string").alias("conditions"),
                # f.array()
                # .cast("array<struct<QCCheckName:string,QCCheckValue:float>>")
                # .alias("sumstatQCValues"),
                # f.lit(None).cast("array<string>").alias("diseaseIds"),
                # f.lit(None).cast("array<string>").alias("backgroundDiseaseIds"),
                "targetsFromSource",
                "molecularComplexId",
            )
        )

    @staticmethod
    def update_study_id(study_id: Column, targets: Column) -> Column:
        """Update study ID to include gene symbol and protein name from target metadata.

        Args:
            study_id (Column): Original study ID column.
            targets (Column): Target metadata column containing gene symbol and protein name.

        Returns:
            Column: Updated study ID column with gene symbol and protein name.

        The updated study ID will have the format:
        {projectId}_{datasourceType}_{aptamerId}_{geneSymbols}_{proteinNames}

        Where geneSymbols and proteinNames are comma-joined values from the targets array.

        In case the geneSymbol or proteinName are missing, we use the placeholder value "_NA"
        to maintain the structure of the study ID.
        """
        study_id_parts = deCODEStudyIdParts.extract_study_id_parts(study_id)
        return f.concat_ws(
            "_",
            study_id_parts.project_id,
            study_id_parts.datasource_type,
            study_id_parts.aptamer_id,
            f.concat_ws(",", f.transform(targets, lambda x: f.coalesce(x.getField("geneSymbol"), f.lit("_NA")))),
            f.concat_ws(",", f.transform(targets, lambda x: f.coalesce(x.getField("proteinId"), f.lit("_NA")))),
        ).alias("updatedStudyId")
